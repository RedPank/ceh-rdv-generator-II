import logging
import re

import pandas as pd
from pandas import DataFrame

from core.config import Config
from core.exceptions import IncorrectMappingException


def _generate_mapping_df(file_data: bytes, sheet_name: str):
    """
    Трансформирует полученные данные EXCEL в тип DataFrame.
    Обрабатываются только данные листа из sheet_name.
    Проверяет в данных наличие колонок из списка.

    Параметры:
        file_data: bytes
            Данные в виде "строки", считанные их EXCEL-файла
        sheet_name: str
            Название листа в книге EXCEL

    Возвращаемое значение:
        Объект с типом DataFrame.
    """

    # Список имен колонок из файла настроек
    columns = Config.excel_data_definition.get('columns', dict())
    # Список колонок в нижнем регистре из файла настроек для листа EXCEL
    columns_list: list[str] = [col_name.lower().strip() for col_name in columns[sheet_name]]

    # Список "псевдонимов" названий колонок из файла настроек
    col_aliases = Config.excel_data_definition.get('col_aliases', dict())
    aliases_list = {key.lower().strip(): val.lower().strip() for key, val in col_aliases[sheet_name].items()}

    # Преобразование данных в DataFrame.
    # Читаем со строки с индексом 1 -> вторая строка сверху.
    try:
        mapping: DataFrame = pd.read_excel(io=file_data, sheet_name=sheet_name, header=1)
    except Exception:
        logging.exception("Ошибка преобразования данных в DataFrame")
        raise

    # Переводим названия колонок в нижний регистр
    # rename_list - словарь {старое_название: новое_название}
    rename_list = {col: col.lower().strip() for col in mapping.columns}
    mapping = mapping.rename(columns=rename_list)

    # Проверка полученных данных
    error: bool = False

    # Находим соответствие между "Названием колонки в программе" и "Названием колонки на листе"
    # Цикл по списку колонок из конфигурационного файла
    for col_name in columns_list:

        if not col_name in mapping.columns.values:

            alias: str | None = aliases_list.get(col_name, None)
            if alias and alias in mapping.columns.values:
                logging.info(f"Имя колонки '{alias}' на листе '{sheet_name}' заменено на '{col_name}'")
                mapping = mapping.rename(columns={alias: col_name})

            else:
                logging.error(f"Колонка '{col_name}' не найдена на листе '{sheet_name}'")
                logging.info("Список допустимых имен колонок:")
                logging.info(columns_list)
                error = True

    if error:
        raise IncorrectMappingException("Ошибка в структуре данных EXCEL")

    # Трансформация данных: оставляем в наборе только колонки из списка и не пустые строки
    mapping = mapping[columns_list].dropna(how='all')

    return mapping


def _is_duplicate(df: pd.DataFrame, field_name: str) -> bool:
    """
    Проверяет колонку DataFrame на наличие не пустых дубликатов

    Args:
        df: DataFrame в котором выполняется проверка
        field_name: Имя поля в DataFrame, для которого выполняется проверка

    Returns:
        object: True-Дубликаты найдены
    """
    return True in df[field_name.lower()].dropna(how='all').duplicated()


def get_duplicate_list(df: pd.DataFrame, field_name: str) -> list | None:
    """
    Проверяет колонку DataFrame на наличие не пустых дубликатов

    Args:
        df: Объект DataFrame, в котором выполняется поиск дубликатов.
        field_name: Имя поля в DataFrame, для которого выполняется поиск.

    Returns:
        object: None, если нет дубликатов или список дубликатов
    """

    dupl = df[field_name].loc[df[field_name].dropna().duplicated()].unique().tolist()
    return dupl


class MappingMeta:
    # Данные листа 'Детали загрузок Src-RDV'
    mapping_df: pd.DataFrame
    # Данные листа 'Перечень загрузок Src-RDV'
    mapping_list: pd.DataFrame

    def __init__(self, byte_data):

        is_error: bool = False
        tgt_pk: set = {'pk', 'bk', 'rk'}

        # Ф-ия для проверки "состава" поля 'tgt_pk'
        def test_tgt_pk(a) -> bool:
            if str is type(a):
                if not a:
                    return True
                else:
                    return len(set(a.split(',')).difference(tgt_pk)) == 0
            else:
                return False

        # Проверка, очистка данных -------------------------------------------------------------------------------------
        # Перечень загрузок Src-RDV ------------------------------------------------------------------------------------
        self.mapping_list = _generate_mapping_df(file_data=byte_data, sheet_name='Перечень загрузок Src-RDV')

        # Удаляем данные, которые не попадают в фильтр из файла конфигурации.
        # Список шаблонов имен потоков и/или имен потоков, которые будут обработаны.
        wf_templates_list = Config.config.get('wf_templates_list', list('.+'))

        # Список потоков, имена которых соответствуют шаблонам
        pattern = '|'.join(wf_templates_list)
        # Оставляем только строки, потоки в которых соответствуют шаблонам
        self.mapping_list = self.mapping_list.query(f"flow_name.str.match('{pattern}')")

        # Заменяем NaN на пустые строки
        self.mapping_list['version_end'] = self.mapping_list['version_end'].fillna(value="")
        self.mapping_list['distribution_field'] = self.mapping_list['distribution_field'].fillna(value="")
        self.mapping_list['comment'] = self.mapping_list['comment'].fillna(value="")


        # Не берем строки, в которых поле version_end не пустое
        self.mapping_list = self.mapping_list.query("version_end == ''")

        # Список целевых таблиц. Проверяем наличие дубликатов в списке
        self._tgt_tables_list: list[str] = self.mapping_list['tgt_table'].dropna().tolist()
        visited: set = set()
        for tbl in self._tgt_tables_list:
            if tbl in visited:
                logging.error(f"На листе 'Перечень загрузок Src-RDV' "
                              f"присутствуют повторяющиеся названия таблиц: {tbl}")
                is_error: bool = True
            else:
                visited.add(tbl)

        # Проверка на наличие дубликатов на листе 'Перечень загрузок Src-RDV'
        for field_name in ['algorithm_uid', 'tgt_table']:
            if _is_duplicate(df=self.mapping_list, field_name=field_name):
                logging.error(f"На листе 'Перечень загрузок Src-RDV' найдены дубликаты в колонке '{field_name}'")
                is_error: bool = True

        # Сортируем по имени потока/алгоритму
        self.mapping_list.sort_values(by=['flow_name', 'algorithm_uid'], inplace=True)

        # Детали загрузок Src-RDV --------------------------------------------------------------------------------------
        self.mapping_df = _generate_mapping_df(file_data=byte_data, sheet_name='Детали загрузок Src-RDV')

        # Оставляем только строки, в которых заполнено поле 'Tgt_table'
        self.mapping_df = self.mapping_df.dropna(subset=['tgt_table'])

        # Заменяем NaN на пустые строки
        self.mapping_df['version_end'] = self.mapping_df['version_end'].fillna(value="")
        # Не берем строки, в которых поле version_end не пустое
        self.mapping_df = self.mapping_df.query("version_end == ''")
        # Оставляем только строки, которые соответствуют "оставленным" потокам
        self.mapping_df = self.mapping_df.query(f"tgt_table in {self._tgt_tables_list}")

        # Преобразуем значения в "нужный" регистр
        self.mapping_df['src_attribute'] = self.mapping_df['src_attribute'].fillna(value="")
        self.mapping_df['src_attribute'] = self.mapping_df['src_attribute'].str.lower()
        self.mapping_df['src_attribute'] = self.mapping_df['src_attribute'].str.strip()

        self.mapping_df['src_attr_datatype'] = self.mapping_df['src_attr_datatype'].fillna(value="")
        self.mapping_df['src_attr_datatype'] = self.mapping_df['src_attr_datatype'].str.lower()
        self.mapping_df['src_attr_datatype'] = self.mapping_df['src_attr_datatype'].str.strip()

        self.mapping_df['expression'] = self.mapping_df['expression'].fillna(value="")
        self.mapping_df['expression'] = self.mapping_df['expression'].str.strip()

        self.mapping_df['tgt_attribute'] = self.mapping_df['tgt_attribute'].fillna(value="")
        self.mapping_df['tgt_attribute'] = self.mapping_df['tgt_attribute'].str.lower()
        self.mapping_df['tgt_attribute'] = self.mapping_df['tgt_attribute'].str.strip()

        self.mapping_df['tgt_attr_datatype'] = self.mapping_df['tgt_attr_datatype'].fillna(value="")
        self.mapping_df['tgt_attr_datatype'] = self.mapping_df['tgt_attr_datatype'].str.lower()
        self.mapping_df['tgt_attr_datatype'] = self.mapping_df['tgt_attr_datatype'].str.strip()

        # Заменяем значения NaN на пустые строки, что-бы дальше "не мучится"
        self.mapping_df['tgt_pk'] = self.mapping_df['tgt_pk'].fillna(value="")
        self.mapping_df['tgt_pk'] = self.mapping_df['tgt_pk'].str.lower()
        self.mapping_df['tgt_pk'] = self.mapping_df['tgt_pk'].str.strip()

        self.mapping_df['comment'] = self.mapping_df['comment'].fillna(value="")
        self.mapping_df['comment'] = self.mapping_df['comment'].str.strip()

        self.mapping_df['attr:conversion_type'] = self.mapping_df['attr:conversion_type'].fillna(value="")
        self.mapping_df['attr:conversion_type'] = self.mapping_df['attr:conversion_type'].str.strip()

        self.mapping_df['attr_nulldefault'] = self.mapping_df['attr_nulldefault'].fillna(value="")
        self.mapping_df['attr_nulldefault'] = self.mapping_df['attr_nulldefault'].str.strip()

        # pattern="^(new_rk|good_default|delete_record)$"
        pattern = Config.get_regexp(name="hub_nulldefault", default="^(new_rk|good_default|delete_record)$")
        # For columns with spaces in their name, you can use backtick quoting.
        err_rows = self.mapping_df.query(f"`attr:conversion_type` == 'hub' and attr_nulldefault != '' and "
                                         f"not attr_nulldefault.str.match('{pattern}')")

        if len(err_rows) > 0:
            logging.error(f"Значение в поле 'attr:nulldefault' не соответствует шаблону: '{pattern}'")
            logging.error('\n' +
                          str(err_rows[['tgt_table', 'tgt_attribute', 'tgt_attr_datatype', 'attr_nulldefault']]))
            is_error = True

        # Проверяем состав поля 'tgt_pk'
        err_rows: pd.DataFrame = self.mapping_df[~self.mapping_df['tgt_pk'].apply(test_tgt_pk)]
        if len(err_rows) > 0:
            logging.error(f"Неверно указаны значения в поле 'tgt_pk'")
            for line in str(err_rows[['tgt_table', 'tgt_attribute', 'tgt_pk', 'tgt_attr_datatype']]).splitlines():
                logging.error(line)
            logging.error(f'Допустимые значения: {tgt_pk}')
            is_error = True

        # "Разворачиваем" колонку Tgt_PK в отдельные признаки
        self.mapping_df = self.mapping_df.assign(_pk=lambda _df: _df['tgt_pk'].str.
                                                 extract('(^|,)(?P<_pk>pk)(,|$)')['_pk'])

        # Признак формирования значения hub из поля _rk/_id
        self.mapping_df = self.mapping_df.assign(_rk=lambda _df: _df['tgt_pk'].str.
                                                 extract(r'(^|,)(?P<_rk>rk|bk)(,|$)')['_rk'])

        # Заменяем значения NaN на пустые строки, что-бы дальше "не мучиться"
        self.mapping_list['scd_type'] = self.mapping_list['scd_type'].fillna(value="")

        # Проверяем поля expression
        exp_err = self.mapping_df.query("expression != '' and src_attribute != ''")
        if len(exp_err) > 0:
            logging.error("Поля 'expression' и 'src_attribut' взаимоисключающие и не могут быть заполнены одновременно")

            logging.error ('Список строк с ошибками:\n' + str(exp_err[['src_table', 'src_attribute', 'expression', 'tgt_table', 'tgt_attribute']]))
            is_error = True

        if is_error:
            raise IncorrectMappingException("Ошибка в структуре данных")


    def get_tgt_tables_list(self) -> list[str]:
        """
        Возвращает список целевых таблиц (из колонки 'tgt_table')
        """
        return self._tgt_tables_list

    def get_mapping_by_tgt_table(self, tgt_table: str) -> pd.DataFrame:
        """
        Возвращает список (DataFrame) строк для заданной целевой таблицы
        """
        df: DataFrame = self.mapping_df[self.mapping_df['tgt_table'] == tgt_table].dropna(how="all")
        return df

    def get_mapping_by_src_table(self, src_table: str) -> pd.DataFrame:
        """
        Возвращает список (DataFrame) строк для заданной целевой таблицы
        """
        df = self.mapping_df[self.mapping_df['src_table'] == src_table].dropna(how="all")
        df = df[['src_table', 'src_attribute', 'src_attr_datatype', 'src_pk', 'comment']]
        return df

    def get_src_cd_by_table(self, tgt_table: str) -> str | None:
        """
        Возвращает наименование источника для заданной целевой таблицы. Если None, то источник не найден
        """
        src_cd_obj = self.mapping_df.query(f'tgt_table == "{tgt_table}" and tgt_attribute == "src_cd"')['expression']
        if len(src_cd_obj) == 0:
            logging.error(f"Не найдено поле 'src_cd' в таблице '{tgt_table}'")
            return None

        if len(src_cd_obj) > 1:
            logging.error(f"Найдено несколько описаний для поля 'src_cd' в таблице '{tgt_table}'")
            return None

        src_cd: str = src_cd_obj.to_numpy()[0]
        # Удаляем пробельные символы
        src_cd = re.sub(r"\s", '', src_cd)
        # Выделяем имя источника
        pattern: str = Config.get_regexp('src_cd_regexp')
        result = re.match(pattern, src_cd)

        if result is None:
            logging.error(f"Не найдено имя источника для таблицы '{tgt_table}' по шаблону '{pattern}'")
            logging.error(f"Найденное значение: {src_cd}")
            logging.info("Имя источника в ячейке EXCEL должно отображаться в формате: ='XXXX'")
            return None

        src_cd = result.groups()[0]
        return src_cd

