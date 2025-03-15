import string
import random

from pandas import Series
from core.config import Config
from core.exceptions import IncorrectMappingException


def create_short_name(name: str, short_name_len: int, random_str_len: int,
                      char_set: str = string.ascii_lowercase + string.digits):
    """
    Функция формирует "короткое имя" на основе значения в переменной name
    Если длина name меньше чем short_name_len, то ф-ия возвращает name без преобразования.
    Если длина name больше чем short_name_len, то name усекается до длинны (short_name_len - random_str_len) и
    дополняется рандомной строкой до длинны short_name_len.
    Args:
        name: Имя, на основе которого надо сформировать "короткое имя".
        short_name_len: Длина "короткого имени", которое надо сформировать.
        random_str_len: Длина рандомной строки, которая используется для формирования "короткого имени".
        char_set: Набор символов, на основе которого формируется рандомная строка.

    Returns: Строка "Короткое имя"
    """

    short_name: str

    if len(name) <= short_name_len:
        short_name = name
    else:
        short_name = name[0:short_name_len - random_str_len]
        short_name = (short_name + ''.join(random.choice(char_set) for _ in range(random_str_len)))

    return short_name


# Класс TargetTable ----------------------------------------------------------------------------------------------------
class DataBaseField:

    def __init__(self, name: str, data_type: str, comment:str, is_nullable: bool, is_pk):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable
        self.comment = comment

        if type(is_pk) is bool:
            self.is_pk = is_pk
        elif type(is_pk) is str:
            self.is_pk = str(is_pk).strip().lower() == 'pk'
        else:
            self.is_pk = False

class TargetTable:

    _ignore_distributed_src: list
    _ignore_hash_fields: list

    def __init__(self, schema: str, table_name: str, comment: str, table_type: str, src_cd: str):

        TargetTable._ignore_distributed_src = Config.setting_up_field_lists.get('ignore_distributed_src', list())
        TargetTable._ignore_hash_fields = Config.setting_up_field_lists.get('ignore_hash_set', list())

        self.fields = []
        self.hash_fields = []
        self.multi_fields = []
        self.hub_fields = []

        self.distributed_by = ''

        self.schema = schema
        self.table_name = table_name
        self.comment = comment
        self.table_type = table_type.upper()
        self.src_cd = src_cd
        self.actual_dttm_name = f"{self.src_cd.lower()}_dttm_name"

        self.file_name = '.'.join([self.schema, self.table_name])

        self.distributed_by = ''

    def add_field(self, field: DataBaseField):
        self.fields.append(field)

        # Список первичных ключей для опции distributed by
        if field.name not in TargetTable._ignore_distributed_src and field.is_pk:
            if self.distributed_by == '':
                self.distributed_by = field.name
            else:
                ','.join([self.distributed_by, field.name])

        # Список полей для расчета hash
        if field.name not in TargetTable._ignore_hash_fields and field.is_pk is False:
            self.hash_fields.append(field.name)

        # Список первичных ключей для опции multi_fields.
        # Поля, которые являются ссылками на hub - не включаются
        if field.is_pk and field.name not in self.hash_fields and field.name not in TargetTable._ignore_distributed_src:
            self.multi_fields.append(field.name)


    def add_hub_field(self):
        pass


class Source:

    def __init__(self, system: str, schema: str, table: str, algorithm_uid:str, algorithm_uid_2: str,
                 ceh_resource: str, src_cd: str, data_capture_mode: str):
        self.system = system
        self.schema = schema
        self.table = table
        self.algorithm_uid=algorithm_uid
        self.algorithm_uid_2=algorithm_uid_2
        self.src_cd = src_cd
        self.data_capture_mode = data_capture_mode

        # Длина short_name должна быть от 2 до 22 символов
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)

        self.uni_res = self.system.lower() + '.' + self.schema.lower() + '.' + self.table.lower()
        self.instance = (self.system + '_' + self.schema).lower()
        self.resource_cd = self.uni_res
        self.actual_dttm_name = f"{src_cd.lower()}_dttm_name"

        self.ceh_res = ceh_resource

        self.file_name = '.'.join([self.system, self.schema, self.table, 'json'])
        self.fields = []

    def add_field(self, field: DataBaseField):
        self.fields.append(field)


class Hub:

    def __init__(self, schema: str, table: str, resource_cd: str):
        self.schema = schema
        self.table = table
        self.resource_cd = resource_cd
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)


class Target:

    def __init__(self, schema: str, table: str, resource_cd: str, src_cd: str):

        self.hubs = []

        self.schema = schema
        self.table = table
        self.resource_cd = resource_cd
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)
        self.src_cd = src_cd

    def add_hub(self, hub: Hub):
        self.hubs.append(hub)


class LocalMetric:

    def __init__(self, processed_dt_conversion: str, processed_dt: str,
                 algo: str, system: str, schema: str, name: str):
        self.processed_dt_conversion = processed_dt_conversion
        self.processed_dt = processed_dt
        self.algo = algo
        self.system = system.lower()
        self.schema=schema.lower()
        self.name=name.lower()

class MartField:

    def __init__(self, tgt_field: str, value_type: str, value: str, tgt_field_type: str):
        self.tgt_field = tgt_field
        self.value_type = value_type
        self.value = value
        self.tgt_field_type = tgt_field_type

    @staticmethod
    def create_mart_field(row: Series):

        src_attr: str = str(row["src_attr"]).strip().lower()
        src_attr_datatype:str = str(row["src_attr_datatype"]).strip().lower()
        tgt_field:str = str(row["tgt_attr"]).strip().lower()
        tgt_field_type:str = str(row["tgt_attr_datatype"]).strip().lower()
        expression: str = str(row["expression"]).strip()

        if expression:
            value_type = "sql_expression"
            # Удаляем знак "=", который должен быть первым
            value = expression[1:].strip()
        elif src_attr_datatype in ["string"] and tgt_field_type in ["text"]:
            value_type = "column"
            value = row["src_attr"]
        elif src_attr_datatype in ["string"] and tgt_field_type in ["timestamp"]:
            value_type = "sql_expression"
            value = src_attr + "::TIMESTAMP"
        elif src_attr_datatype in ["string"] and tgt_field_type in ["date"]:
            value_type = "sql_expression"
            value = src_attr + "::DATE"
        else:
            value_type = "column"
            value = src_attr

        fld = MartField(tgt_field=tgt_field, value_type = value_type, value=value, tgt_field_type=tgt_field_type.upper())
        return fld

class MartHub:
    def __init__(self, hub_target: str, rk_field: str, business_key_schema: str, on_full_null: str, field: str,
                 src_type: str, field_type: str, is_bk: bool, schema: str):

        self.schema = schema            # Схема в базе данных, где расположена хаб-таблица
        self.hub_target = hub_target    # Имя хаб-таблицы без схемы
        self.hub_name_only = self.hub_target
        self.full_table_name = self.schema + '.' + self.hub_target

        self.rk_field = rk_field
        self.id_field = self.rk_field.removesuffix('_rk') + '_id'

        self.business_key_schema = business_key_schema
        self.on_full_null = on_full_null
        self.field = field
        self.src_type = src_type
        self.field_type = field_type
        self.is_bk = 'true' if is_bk else 'false'
        self.src_cd = ''
        self.actual_dttm_name = ''

class Mart:
    # Список полей целевой таблицы, которые не будут добавлены в секцию field_map шаблона wf.yaml
    _ignore_field_map_ctx_list: dict

    def __init__(self, short_name: str, algorithm_uid: str, algorithm_uid_2: str, target: str, source: str,
                 delta_mode: str, processed_dt: str, algo: str, source_system: str, source_schema: str, source_name: str,
                 actual_dttm_name: str, src_cd: str):

        Mart._ignore_field_map_ctx_list = Config.setting_up_field_lists.get('ignore_field_map_ctx_list', dict())

        self.fields_map = []
        self.mart_hub_list = []

        self.short_name = short_name
        self.algorithm_uid = algorithm_uid
        self.algorithm_uid_2 = algorithm_uid_2
        self.target = target
        self.source = source
        self.delta_mode = delta_mode
        self.processed_dt = processed_dt
        self.algo = algo
        self.source_system = source_system.lower()
        self.source_schema = source_schema.lower()
        self.source_name = source_name.lower()
        self.actual_dttm_name = actual_dttm_name.lower()
        self.src_cd = src_cd


        # Список полей с описанием, которые БУДУТ добавлены в секцию field_map шаблона wf.yaml
        add_field_map_ctx_lis: dict = Config.setting_up_field_lists.get('add_field_map_ctx_list', dict())
        if type(add_field_map_ctx_lis) is dict:
            tgt_field_keys = add_field_map_ctx_lis.keys()
            for tgt_field in tgt_field_keys:
                # Описание поля
                fld = add_field_map_ctx_lis[tgt_field]
                mart_field = MartField(tgt_field=tgt_field, value_type=fld.type, value=fld.value,
                                       tgt_field_type=fld.field_type.upper())
                self.add_fields_map(mart_field)

    def add_fields_map(self, mart_field: MartField):

        #  Не добавляем поля из списка "ignore_field_map_ctx_list"
        if mart_field.tgt_field in Mart._ignore_field_map_ctx_list:
            return

        # Проверяем, если поле уже присутствует, то выдается ошибка
        if [ctx.tgt_field for ctx in self.fields_map if ctx.tgt_field == mart_field.tgt_field]:
            msg = f'Поле "{mart_field.tgt_field}" уже присутствует в списке полей'
            raise IncorrectMappingException(msg)

        self.fields_map.append(mart_field)

    def add_mart_hub_list(self, mart_hub: MartHub):
        mart_hub.actual_dttm_name = self.actual_dttm_name
        mart_hub.src_cd = self.src_cd
        self.mart_hub_list.append(mart_hub)


class FlowContext:

    author: str
    flow_name : str
    base_flow_name: str
    data_capture_mode: str
    delta_mode: str


    def __init__(self, flow_name : str):

        self.tags = []
        self.resource_tags = []
        self.sources = []
        self.targets = []
        self.local_metrics = []
        self.marts = []
        self.target_tables = []
        self.hubs = []

        self.flow_name = flow_name
        self.base_flow_name = flow_name.removeprefix('wf_')

        self.processed_dt: str = Config.setting_up_field_lists.get('processed_dt', 'processed_dt')
        self.processed_dt_conversion = Config.setting_up_field_lists.get('processed_dt_conversion', 'second')
        self.tgt_history_field = Config.setting_up_field_lists.get('tgt_history_field', '')


    def add_source(self, source: Source):
        self.sources.append(source)

    def add_target(self, target: Target):
        self.targets.append(target)

    def add_local_metric(self, local_metric: LocalMetric):
        self.local_metrics.append(local_metric)

    def add_mart(self, mart: Mart):
        self.marts.append(mart)

        # Формируем уникальный список хабов потока
        for hub in mart.mart_hub_list:
            if not [ctx.full_table_name for ctx in self.hubs if ctx.full_table_name == hub.full_table_name]:
                self.hubs.append(hub)


    def tags_formation(self):

        cfg_tags = Config.resource_tags
        if type(cfg_tags) is list:
            for tag in cfg_tags:
                if type(tag) is dict:
                    self.resource_tags.append('"' + list(tag.keys())[0] + '"' + ':' + '"' + list(tag.values())[0]+ '"')
                else:
                    self.resource_tags.append('"' +tag + '"')

        # Добавляем строки из файла конфигурации
        cfg_tags = Config.tags
        if type(cfg_tags) is list:
            for tag in cfg_tags:
                if type(tag) is dict:
                    self.tags.append('"' + list(tag.keys())[0] + '"' + ':' + '"' + list(tag.values())[0] + '"')
                else:
                    self.tags.append('"' + tag + '"')

        # Добавляем динамические строки
        self.tags.append('src_cd: ' + self.targets[0].src_cd.upper())
        self.tags.append('prv: ' + self.sources[0].system)
        self.tags.append('tgt: ' + self.targets[0].schema)
        self.tags.append('cf_' + self.base_flow_name)
        self.tags.append('wf_' + self.base_flow_name)

        # Источники
        for src in self.sources:
            self.tags.append('src_tbl: ' + src.schema + '.' + src.table)
        # Алгоритм
        for src in self.sources:
            self.tags.append('UID: ' + src.algorithm_uid)

        # Целевая таблица
        for tgt in self.targets:
            if tgt.table.startswith('mart_'):
                self.tags.append('tgt_tbl: ' + tgt.table)


    def add_target_table(self, target_table: TargetTable):
        target_table.multi_fields.sort()
        target_table.hash_fields.sort()
        self.target_tables.append(target_table)
