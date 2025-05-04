import copy
import io
import logging
import os
import re

from pandas import DataFrame

from core import mapping
from core.config import Config
from core.exportdata import ExportData
from core.flowcontext import FlowContext, Source, Target, Mart, MartField, HubMartField, LocalMetric, TargetTable, \
    DataBaseField
from core.mapping import MappingMeta
from core.stream_header_data import StreamHeaderData


def mapping_generator(file_path: str, out_path: str) -> None:
    """Функция считывает данные из EXCEL, составляет список потоков и запускает процесс формирования файлов для каждого
     потока

    Args:
        file_path (str): Полный путь к файлу маппинга RDV
        out_path (str): Каталог, в котором будут сформированы подкаталоги с описанием потоков
    """

    Config.is_warning = False
    Config.is_error = False

    logging.info(f"file_path: {file_path}")
    logging.info(f"out_path: {out_path}")
    logging.info(f"author: {Config.author}")

    logging.info(f'Чтение данных из файла "{file_path}"')

    # Чтение данных их EXCEL
    try:
        with open(file_path, 'rb') as f:
            byte_data = io.BytesIO(f.read())

    except FileNotFoundError:
        logging.error(f"Не найден файл '{file_path}'")
        Config.is_error = True
        return

    except Exception as err:
        msg = f"Ошибка чтения данных из файла {file_path}"
        logging.exception(msg)
        Config.is_error = True
        raise err

    src_attr_datatypes = Config.field_type_list['src_attr_datatype']
    tgt_attr_datatypes = Config.field_type_list['tgt_attr_datatype']
    tgt_attr_predefined_datatypes: dict = Config.field_type_list['tgt_attr_predefined_datatype']
    ignore_field_map_ctx_list: dict = Config.setting_up_field_lists['ignore_field_map_ctx_list']

    tgt_attr_name_regexp_pattern: str = Config.get_regexp('tgt_attr_name_regexp')

    pattern_bk_schema: str = Config.get_regexp('bk_schema_regexp')
    pattern_bk_object: str = Config.get_regexp('bk_object_regexp')

    processed_dt = Config.config.get('processed_dt', 'processed_dt')
    processed_dt_conversion = Config.config.get('processed_dt_conversion', 'second')

    corresp_datatype: dict = Config.field_type_list.get('corresp_datatype', dict())
    if len(corresp_datatype) == 0:
        Config.is_warning = True
        logging.warning('Не найден параметр "corresp_datatype" в файле конфигурации')
        logging.warning("Проверка соответствия типов полей источника и целевой таблицы производится не будет")

    # Данные EXCEL
    mapping_meta = MappingMeta(byte_data)

    # Цикл по списку потоков
    flow_list = mapping_meta.mapping_list['flow_name'].unique()

    logging.info('')
    logging.info('Формирование файлов описания потоков ...')

    if len(flow_list) == 0:
        logging.warning("Ни один из потоков не будет сформирован, т.к. не найдено соответствие имени потока шаблонам")
        logging.warning("Проверьте список шаблонов в секции 'wf_templates_list' в файле конфигурации")
        logging.info('')
        Config.is_warning = True
        return

    for wrk_index in range(len(flow_list)):

        flow_name = flow_list[wrk_index]
        is_table_error = False

        flow_context = FlowContext(flow_name)

        # Цикл по списку целевых таблиц
        table_index: int = 0
        for _, row in mapping_meta.mapping_list.query(f'flow_name == "{flow_name}"').iterrows():

            logging.info('')
            logging.info(f">>>>> Поток: {wrk_index + 1}: {flow_name}")
            logging.info(f">>>>> Таблица: {table_index + 1}: {row["tgt_table"]}")
            table_index =+ 1

            # Данные строки "Перечень загрузок Src-RDV" листа для таблицы
            sh_data = StreamHeaderData(row=row)

            # Умеем работать только с MART-таблицами
            if sh_data.target_rdv_object_type != 'MART':
                logging.error(f"Программа не поддерживает обработку целевых объектов с типом "
                              f"{sh_data.target_rdv_object_type}")
                Config.is_error = True
                continue

            # Полное имя таблицы: схема + имя_таблицы
            tgt_full_name = sh_data.tgt_full_name

            # Данные для заданной целевой таблицы
            tgt_mapping: DataFrame = mapping_meta.get_mapping_by_tgt_table(tgt_full_name)

            logging.info('')

            if len(tgt_mapping) == 0:
                msg = f"Не найдена таблица {tgt_full_name} на листе 'Детали загрузок Src-RDV'"
                logging.error(msg)
                is_table_error = True
                continue

            # Проверяем таблицу-источник
            pattern: str = Config.get_regexp('src_table_name_regexp')
            if not re.match(pattern, sh_data.src_full_name):
                logging.error(f'Имя таблицы-источника "{sh_data.src_full_name}" на листе "Перечень загрузок Src-RDV"'
                              f' не соответствует шаблону "{pattern}"')
                is_table_error = True
                continue

            logging.info(f'Таблица-источник: {sh_data.src_full_name}')

            # Проверяем соответствие названия целевой таблицы шаблону
            pattern: str = Config.get_regexp('tgt_table_name_regexp')
            if not re.match(pattern, tgt_full_name):
                logging.error(f'Целевая таблица: "{tgt_full_name}" на листе "Перечень загрузок Src-RDV" '
                              f'не соответствует шаблону "{pattern}"')
                is_table_error = True
                continue

            logging.info(f'Целевая таблица: {tgt_full_name}')

            # Возвращает наименование (логическое) "источника" для заданной целевой таблицы - поле src_sd
            src_cd: str | None = mapping_meta.get_src_cd_by_table(tgt_full_name)
            if not src_cd:
                logging.error(f'Для целевой таблицы "{tgt_full_name}" неверно задано/не задано имя источника')
                logging.error('Имя источника задается в колонке "Expression" для поля "src_cd"')
                is_table_error = True
                continue

            logging.info(f'Источник данных (src_cd): {src_cd}')

            # Имя источника - Source_name
            if not sh_data.source_system:
                logging.error(f'Для таблицы {tgt_full_name} неверно задано/не задано поле "Источник данных'
                              f' (транспорт)"/Source_name')
                is_table_error = True
                continue
            logging.info(f'Система - Источник данных: {sh_data.source_system}')

            # Алгоритм - Algorithm_UID
            algorithm_uid: str = sh_data.algorithm_uid
            if not algorithm_uid:
                logging.error(f'Для таблицы {tgt_full_name} неверно задано/не задано поле "UID алгоритма"/"Algorithm_UID"')
                is_table_error = True
                continue
            logging.info(f'Код алгоритма: {algorithm_uid}')

            subalgorithm_uid: str = str(sh_data.subalgorithm_uid).strip()
            if not subalgorithm_uid:
                logging.error(f'Для таблицы {tgt_full_name} неверно задано/не задано поле "UID субалгоритма"/"Sub_UID"')
                is_table_error = True
                continue

            if not subalgorithm_uid.isdigit():
                logging.error(f'Поле "UID суб-алгоритма"/"Sub_UID" должно быть целым числом')
                logging.error(f'Код суб-алгоритма: "{subalgorithm_uid}"')
                is_table_error = True
                continue

            logging.info(f'Код суб-алгоритма: "{subalgorithm_uid}"')

            ceh_resource: str = "ceh." + tgt_full_name

            # Внешняя таблица - источник
            source = Source(system=sh_data.source_system, schema=sh_data.src_schema, table=sh_data.src_table,
                            algorithm_uid=algorithm_uid, algorithm_uid_2=subalgorithm_uid, ceh_resource=ceh_resource,
                            src_cd=src_cd, data_capture_mode=Config.data_capture_mode)

            # Поля таблицы - источника
            src_mapping = mapping_meta.get_mapping_by_src_table(src_table=sh_data.src_full_name)
            # Список полей - дубликатов в источнике
            dupl = mapping.get_duplicate_list(df=src_mapping, column_name='src_attribute')

            if len( dupl ) > 0:
                logging.warning(f"В таблице-источнике {sh_data.src_full_name} указаны повторяющиеся названия полей")
                logging.warning(str(dupl))
                logging.warning('При формировании файла описания таблицы источника дубликаты будут удалены')
                Config.is_warning = True

            # Удаляем дубликаты имен полей из списка полей таблицы-источника
            src_mapping = src_mapping.drop_duplicates(subset=['src_attribute'], keep='first')
            # Формируем список полей источника
            for s_index, s_row in src_mapping.iterrows():

                # Проверяем типы полей источника
                if s_row['src_attr_datatype'] not in src_attr_datatypes:
                    logging.warning(f'Тип поля "{s_row['src_attribute']}" в таблице источнике "{s_row['src_attr_datatype']}" '
                                    f'не входит в список разрешенных типов')
                    Config.is_warning = True

                source.add_field(DataBaseField(name=s_row['src_attribute'], data_type=s_row['src_attr_datatype'],
                                               comment=s_row['comment'], is_nullable=False, is_pk=s_row['src_pk'],
                                               properties = dict()))

            flow_context.add_source(source)

            # Целевая таблица
            target = Target(schema=sh_data.tgt_schema, table = sh_data.tgt_table, src_cd=src_cd.lower(),
                            object_type=sh_data.target_rdv_object_type)

            # Секция "local_metrics". Данные формируются для каждой таблицы. Но используется перове значение.
            local_metric: LocalMetric = (
                LocalMetric(processed_dt_conversion=processed_dt_conversion,
                            processed_dt=processed_dt,
                            algo=sh_data.algorithm_uid,
                            system=sh_data.source_system,
                            schema=sh_data.src_schema,
                            name=sh_data.src_table))
            flow_context.add_local_metric(local_metric=local_metric)

            flow_context.add_target(target)

            delta_mode = Config.config.get("delta_mode", 'new')
            actual_dttm_name = f"{src_cd.lower()}_actual_dttm"

            mart_mapping: Mart = (
                Mart(short_name=target.short_name, algorithm_uid=sh_data.algorithm_uid,
                     algorithm_uid_2=sh_data.subalgorithm_uid, target=target.short_name, source=source.short_name,
                     delta_mode=delta_mode, processed_dt=processed_dt, algo=sh_data.algorithm_uid,
                     source_system=sh_data.source_system, source_schema=sh_data.src_schema,
                     source_name=sh_data.src_table,
                     table_name=sh_data.tgt_table,
                     actual_dttm_name=actual_dttm_name, src_cd=src_cd, comment=sh_data.comment)
            )

            # Цикл по полям целевой таблицы
            for f_index, f_row in tgt_mapping.iterrows():
                mart_field = MartField.create_mart_field(f_row)
                mart_mapping.add_fields(copy.copy(mart_field))

                if mart_field.is_hub_field:
                    logging.warning(f"Поле '{mart_field.tgt_field}' не будет добавлено в секцию 'field_map', "
                                    f"т.к. присутствует в секции 'hub_map'")

                # Проверяем типы полей целевой таблицы
                if f_row['tgt_attr_datatype'] not in tgt_attr_datatypes:
                    logging.warning(f'Тип поля "{f_row['tgt_attribute']}"/"{f_row['tgt_attr_datatype']}" в целевой таблице '
                                    f'не входит в список разрешенных типов')
                    Config.is_warning = True

                # Проверяем соответствие типа данных полей источника и целевой таблицы
                if len (corresp_datatype) != 0  and f_row['src_attribute'] != '':
                    if f_row['src_attr_datatype'] not in corresp_datatype:
                        logging.warning(f"Тип данных поля источника '{f_row['src_attr_datatype']}' отсутствует в 'corresp_datatype'")
                        Config.is_warning= True

                    elif f_row["tgt_attr_datatype"] not in corresp_datatype[f_row['src_attr_datatype']]:
                        logging.warning(f"Тип данных '{f_row['tgt_attr_datatype']}' поля целевой таблицы "
                                      f"'{f_row['tgt_attribute']}' "
                                      f"не найден списке в 'corresp_datatype' "
                                      f"{f_row['src_attr_datatype']}:{corresp_datatype[f_row['src_attr_datatype']]}")
                        Config.is_warning = True

                # Проверяем соответствие названия полей целевой таблицы шаблону
                if not re.match(tgt_attr_name_regexp_pattern, f_row['tgt_attribute']):
                    logging.error(f"Название поля целевой таблицы {f_row['tgt_attribute']} "
                                  f"не соответствует шаблону '{tgt_attr_name_regexp_pattern}'")
                    Config.is_error = True

                # Проверяем наличие данных для полей целевой таблицы
                if not f_row['src_attribute'] and not f_row['expression']:
                    if not f_row['tgt_attribute'] in ignore_field_map_ctx_list:
                        logging.warning(f"Для поля {f_row['tgt_attribute']} целевой таблицы не указано "
                                        f"поле в источнике (src_attr) или расчетное значение (expression)")
                        Config.is_warning = True

            # Проверка полей целевой таблицы, тип которых фиксирован
            for fld_name in tgt_attr_predefined_datatypes.keys():
                err_rows = tgt_mapping.query(f"tgt_attribute == '{fld_name}'")[['tgt_attribute', 'tgt_attr_datatype', 'tgt_attr_mandatory']]
                if len(err_rows) == 0:
                    logging.error(f"Не найден обязательный атрибут '{fld_name}'")
                    Config.is_error = True

                elif len(err_rows) > 1:
                    logging.error(f"Обязательный атрибут '{fld_name}' указан более одного раза\n" + str(err_rows))
                    Config.is_error = True

                else:
                    if (err_rows.iloc[0]['tgt_attr_datatype'] != tgt_attr_predefined_datatypes[fld_name][0] or
                            err_rows.iloc[0]['tgt_attr_mandatory'] != tgt_attr_predefined_datatypes[fld_name][1]):
                        logging.error(
                            f"Параметры обязательного атрибута '{fld_name}' указаны неверно\n" + str(err_rows))

                        Config.is_error = True


            #  Описание MART - таблицы со всеми "вложениями"
            target_table = TargetTable(schema=sh_data.tgt_schema, table_name=sh_data.tgt_table, comment=sh_data.comment,
                                       table_type=sh_data.target_rdv_object_type, src_cd=src_cd,
                                       distribution_field=sh_data.distribution_field)

            #  Список полей целевой таблицы
            for f_index, f_row in tgt_mapping.iterrows():

                properties = dict()
                if f_row["attr:conversion_type"] == 'hub':
                    properties["is_hub_field"] = True
                    properties["hub"] = []

                    # Контроль названия бк-схемы и имени хаба
                    bk_schema = f_row['attr:bk_schema']
                    if not re.match(pattern_bk_schema, bk_schema):
                        logging.error(
                            f'Имя бк-схемы "{bk_schema}" на листе "Детали загрузок Src-RDV"'
                            f' не соответствует шаблону "{pattern_bk_schema}"')
                        is_table_error = True

                    bk_object:str = f_row['attr:bk_object']
                    if not re.match(pattern_bk_object, bk_object):
                        logging.error(
                            f'Имя хаба "{bk_object}" на листе "Детали загрузок Src-RDV"'
                            f' не соответствует шаблону "{pattern_bk_object}"')
                        is_table_error = True

                    # Если rk-поле прописано в "attr:bk_object", то берем его оттуда
                    rk_field = f_row['tgt_attribute'] if len(f_row['attr:bk_object'].split('.')) == 2 else f_row['attr:bk_object'].split('.')[2]

                    mart_hub = HubMartField(rk_field=rk_field, hub_table=f_row['attr:bk_object'].split('.')[1],
                                            business_key_schema=f_row['attr:bk_schema'],
                                            on_full_null=f_row['attr_nulldefault'], src_attribute=f_row['src_attribute'],
                                            src_type=f_row['src_attr_datatype'], expression=f_row['expression'],
                                            field_type=f_row['tgt_attr_datatype'],
                                            is_bk=f_row['is_pk'], schema=f_row['attr:bk_object'].split('.')[0],
                                            mart_retain_key=f_row['tgt_attribute'])

                    # Привязываем hub к описанию mart-таблицы
                    mart_mapping.add_mart_hub_list(mart_hub=mart_hub)
                    # Привязываем hub к целевой таблице
                    target_table.add_hub_field(mart_hub)


                # Привязываем поле к целевой таблице
                data_base_field = DataBaseField(name=f_row["tgt_attribute"], data_type=f_row['tgt_attr_datatype'],
                                                comment=f_row["comment"],
                                                is_nullable=f_row["tgt_attr_mandatory"] == 'null',
                                                is_pk=f_row["is_pk"],
                                                properties=properties)

                target_table.add_field(field=data_base_field)


            flow_context.add_target_table(target_table=target_table)

            # Список полей для расчета hash, проверка количества
            if len(target_table.hash_fields) > 100:
                logging.warning(f"Количество полей для расчета hash_diff в таблице {target_table.table_name} более 100")
                Config.is_warning = True

            # Добавляем описание mart к потоку
            flow_context.add_mart(mart_mapping)

        # Конец цикла по списку таблиц #################################################################################

        if is_table_error:
            Config.is_error = True
            logging.error(f'Файлы потока "{flow_name}" не были сформированы!')
            continue

        # Секция tags формируется последней
        flow_context.tags_formation()
        flow_context.author = Config.author
        flow_context.data_capture_mode = Config.data_capture_mode
        flow_context.delta_mode = Config.delta_mode

        # Вывод информации в файл
        # Каталог для файлов потока
        out_path_flow = os.path.join(out_path, flow_name)
        logging.info(f'Каталог потока: {out_path_flow}')

        export_data = ExportData(templates_path=Config.templates_path, path=out_path_flow, flow_context=flow_context)

        # Формируем файлы описания потока
        export_data.generate_files()

        logging.info(f'Файлы потока "{flow_name}" сформированы')


    # Конец цикла по списку потоков #№№№################################################################################

    if Config.is_error:
        logging.error(f'Один или более потоков не были сформированы из-за обнаруженных ошибок')

    logging.info('')
