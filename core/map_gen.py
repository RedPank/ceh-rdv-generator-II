import copy
import io
import logging
import os
import re

import pandas as pd
from pandas import DataFrame

from core.config import Config
from core.exportdata import ExportData
from core.flowcontext import FlowContext, Source, Target, Hub, Mart, MartField, MartHub, LocalMetric, TargetTable, \
    DataBaseField
from core.mapping import MappingMeta
from core.stream_header_data import StreamHeaderData


def mapping_generator(
        file_path: str,
        out_path: str
) -> None:
    """Функция считывает данные из EXCEL, составляет список потоков и запускает процесс формирования файлов для каждого
     потока

    Args:
        file_path (str): Полный путь к файлу маппинга РДВ
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

    except Exception as ex:
        msg = f"Ошибка чтения данных из файла {file_path}"
        logging.exception(msg)
        Config.is_error = True
        raise ex

    # Данные EXCEL
    mapping_meta = MappingMeta(byte_data)

    # Цикл по списку потоков
    flow_list = mapping_meta.mapping_list['flow_name'].unique()

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
        for index, row in mapping_meta.mapping_list.query(f'flow_name == "{flow_name}"').iterrows():
            if wrk_index > 0:
                logging.info('')

            # Данные "Перечень загрузок Src-RDV" листа для таблицы
            sh_data = StreamHeaderData(row=row)
            tgt_full_name = sh_data.tgt_full_name

            # Данные для заданной целевой таблицы
            tgt_mapping: DataFrame = mapping_meta.get_mapping_by_tgt_table(tgt_full_name)

            logging.info('')
            logging.info('>>>>> Begin >>>>>')
            logging.info(f"{wrk_index+1}: {sh_data.flow_name}")

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
                is_table_error = True
                continue

            logging.info(f'Код суб-алгоритма: {subalgorithm_uid}')

            ceh_resource: str = "ceh." + tgt_full_name

            source = Source(system=sh_data.source_system, schema=sh_data.src_schema, table=sh_data.src_table,
                            algorithm_uid=algorithm_uid, algorithm_uid_2=subalgorithm_uid, ceh_resource=ceh_resource,
                            src_cd=src_cd, data_capture_mode=Config.data_capture_mode)

            src_mapping = mapping_meta.get_mapping_by_src_table(src_table=sh_data.src_full_name)
            for s_index, s_row in src_mapping.iterrows():
                source.add_field(DataBaseField(name=s_row['src_attribute'], data_type=s_row['src_attr_datatype'],
                                               comment=s_row['comment'], is_nullable=False, is_pk=s_row['src_pk']))

            flow_context.add_source(source)

            target = Target(schema=sh_data.tgt_schema, table = sh_data.tgt_table,
                                    resource_cd=sh_data.tgt_resource_cd, src_cd=src_cd.lower())
            # Список хабов
            hubs: pd.DataFrame = tgt_mapping[tgt_mapping['attr:conversion_type'] == 'hub']
            hubs = hubs[['tgt_attribute', 'attr:bk_schema', 'attr:bk_object', 'attr:nulldefault', 'src_attribute',
                       'expression', 'tgt_pk', 'tgt_attr_datatype', '_pk', 'src_attr_datatype', 'tgt_attr_mandatory']]

            pattern_bk_schema: str = Config.get_regexp('bk_schema_regexp')
            pattern_bk_object: str = Config.get_regexp('bk_object_regexp')
            for h_index, h_row in hubs.iterrows():

                # Контроль названия бк-схемы
                bk_schema = h_row['attr:bk_schema']
                if not re.match(pattern_bk_schema, bk_schema):
                    logging.error(
                        f'Имя бк-схемы "{bk_schema}" на листе "Детали загрузок Src-RDV"'
                        f' не соответствует шаблону "{pattern_bk_schema}"')
                    is_table_error = True

                bk_object:str = h_row['attr:bk_object']
                if not re.match(pattern_bk_object, bk_object):
                    logging.error(
                        f'Имя хаба "{bk_object}" на листе "Детали загрузок Src-RDV"'
                        f' не соответствует шаблону "{pattern_bk_object}"')
                    is_table_error = True

                target.add_hub(Hub(schema=bk_object.split('.')[0], table = bk_object.split('.')[1],
                                   resource_cd='ceh.'+bk_object))

            flow_context.add_target(target)

            processed_dt = Config.config.get('processed_dt', 'processed_dt')
            processed_dt_conversion = Config.config.get('processed_dt_conversion', 'second')

            local_metric: LocalMetric = (
                LocalMetric(processed_dt_conversion=processed_dt_conversion,
                            processed_dt=processed_dt,
                            algo=sh_data.algorithm_uid,
                            system=sh_data.source_system,
                            schema=sh_data.src_schema,
                            name=sh_data.src_table))
            flow_context.add_local_metric(local_metric=local_metric)

            delta_mode = Config.config.get("delta_mode", 'new')
            actual_dttm_name = f"{src_cd.lower()}_dttm_name"

            mart_mapping: Mart = (
                Mart(short_name=target.short_name, algorithm_uid=sh_data.algorithm_uid,
                     algorithm_uid_2=sh_data.subalgorithm_uid, target=target.short_name, source=source.short_name,
                     delta_mode=delta_mode, processed_dt=processed_dt, algo=sh_data.algorithm_uid,
                     source_system=sh_data.source_system, source_schema=sh_data.src_schema,
                     source_name=sh_data.src_table,
                     actual_dttm_name=actual_dttm_name, src_cd=src_cd, comment=sh_data.comment)
            )


            for f_index, f_row in tgt_mapping.iterrows():
                # mart_field: MartField = (
                #     MartField(value=f_row["src_attribute"], expression=f_row["expression"],
                #               value_type=f_row["src_attr_datatype"],
                #               tgt_field_type=f_row["tgt_attr_datatype"], tgt_field=f_row["tgt_attribute"],))

                mart_field = MartField.create_mart_field(f_row)
                mart_mapping.add_fields_map(copy.copy(mart_field))


            for h_index, h_row in hubs.iterrows():

                mart_hub = MartHub(rk_field=h_row['tgt_attribute'], hub_target=h_row['attr:bk_object'].split('.')[1],
                                   business_key_schema=h_row['attr:bk_schema'],
                                   on_full_null=h_row['attr:nulldefault'], src_attribute=h_row['src_attribute'],
                                   src_type=h_row['src_attr_datatype'], expression=h_row['expression'],
                                   field_type=h_row['tgt_attr_datatype'],
                                   is_bk=h_row['_pk'] == 'pk', schema=h_row['attr:bk_object'].split('.')[0])
                mart_mapping.add_mart_hub_list(mart_hub=mart_hub)

                bk_object:str = h_row['attr:bk_object']
                target.add_hub(Hub(schema=bk_object.split('.')[0], table = bk_object.split('.')[1],
                                   resource_cd='ceh.'+bk_object))


            flow_context.add_mart(copy.copy(mart_mapping))

            target_table = TargetTable(schema=sh_data.tgt_schema, table_name=sh_data.tgt_table, comment=sh_data.comment,
                                       table_type=sh_data.target_rdv_object_type, src_cd=src_cd,
                                       distribution_field=sh_data.distribution_field)
            for f_index, f_row in tgt_mapping.iterrows():
                data_base_field = DataBaseField(name=f_row["tgt_attribute"], data_type=f_row['tgt_attr_datatype'],
                                                comment=f_row["comment"],
                                                is_nullable=f_row["tgt_attr_mandatory"] == 'null',
                                                is_pk=f_row["_pk"] == 'pk')
                target_table.add_field(field=data_base_field)

            flow_context.add_target_table(target_table=target_table)

            if len(target_table.hash_fields) > 100:
                logging.warning(f"Количество полей для расчета hash_diff в таблице {target_table.table_name} более 100")
                Config.is_warning = True

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

        export_data = ExportData(templates_path=Config.templates_path, path=out_path_flow)
        export_data.flow_context = flow_context

        # Формируем файлы описания потока
        export_data.generate_files()

        logging.info(f'Файлы потока "{flow_name}" сформированы')


    # Конец цикла по списку потоков #№№№################################################################################

    if Config.is_error:
        logging.error(f'Один или более потоков не были сформированы из-за обнаруженных ошибок')

    logging.info('')
