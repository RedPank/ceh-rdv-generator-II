import random
import string
from datetime import datetime

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

    _ignore_primary_key: list
    _ignore_hash_fields: list

    def __init__(self, schema: str, table_name: str, comment: str, table_type: str, src_cd: str,
                 distribution_field: str):

        TargetTable._ignore_primary_key = Config.setting_up_field_lists.get('ignore_primary_key', list())
        TargetTable._ignore_hash_fields = Config.setting_up_field_lists.get('ignore_hash_set', list())

        self.fields = []
        self.hash_fields = []
        self.multi_fields = []
        # self.hub_fields = []

        self.distributed_by = ''
        self.primary_key = ''

        self.schema = schema
        self.table_name = table_name
        self.comment = comment
        self.table_type = table_type.upper()
        self.src_cd = src_cd
        self.actual_dttm_name = f"{self.src_cd.lower()}_dttm_name"

        self.file_name = '.'.join([self.schema, self.table_name])
        self.distribution_field= distribution_field.lower()
        self.distributed_by = self.distribution_field

    def add_field(self, field: DataBaseField):
        self.fields.append(field)

        if field.name not in TargetTable._ignore_primary_key and field.is_pk:
            self.primary_key = self.primary_key + ',' + field.name if self.primary_key else field.name

        # Список первичных ключей для опции distributed by.
        # Заполняется только если колонка EXCEL "Distribution_field" - пустая
        if not self.distribution_field:
            self.distributed_by = self.primary_key

        # Список полей для расчета hash
        if field.name not in TargetTable._ignore_hash_fields and field.is_pk is False:
            self.hash_fields.append(field.name)

        # Список первичных ключей для опции multi_fields.
        # Поля, которые являются ссылками на hub - не включаются
        if field.is_pk and field.name not in self.hash_fields and field.name not in TargetTable._ignore_primary_key:
            self.multi_fields.append(field.name)


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

    def __init__(self, schema: str, table: str):
        self.schema = schema
        self.table = table
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)


class Target:

    def __init__(self, schema: str, table: str, src_cd: str, object_type: str, resource_cd: str = None):

        self.hubs = []

        self.schema = schema
        self.table = table
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)
        self.src_cd = src_cd
        self.object_type = object_type
        self.resource_cd = resource_cd if resource_cd is not None else '.'.join(['ceh', self.schema, self.table])

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

    def __init__(self, tgt_field: str, value_type: str, value: str, expression: str, tgt_field_type: str):
        self.tgt_field = tgt_field
        self.value_type = value_type
        self.value = value
        self.expression = expression
        self.tgt_field_type = tgt_field_type
        self.is_hub_field = False

    @staticmethod
    def create_mart_field(row: Series):

        src_attr: str = str(row["src_attribute"]).strip().lower()
        src_attr_datatype:str = str(row["src_attr_datatype"]).strip().lower()
        tgt_field:str = str(row["tgt_attribute"]).strip().lower()
        tgt_field_type:str = str(row["tgt_attr_datatype"]).strip().lower()
        expression: str = str(row["expression"]).strip().removeprefix('=')
        # not_null:bool = str(row["tgt_attr_mandatory"]).strip().lower() == 'not null'
        is_pk:bool = str(row["_pk"]).strip().lower() == 'pk'

        value = src_attr

        if expression:
            value_type = "sql_expression"
            expression = expression + ' :: ' + tgt_field_type.upper()

        elif src_attr_datatype in ["string"] and tgt_field_type in ["text"] and not is_pk:
            value_type = "sql_expression"
            expression = f"case when {src_attr} = '' then Null else {src_attr} end"  + ' :: ' + tgt_field_type.upper()

        elif src_attr_datatype in ["string"] and tgt_field_type in ["timestamp"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2ts({src_attr})"

        elif src_attr_datatype in ["string"] and tgt_field_type in ["date"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2dt({src_attr})"

        elif src_attr_datatype in ["string"] and tgt_field_type in ["smallint", "int2"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2int2({src_attr})"

        elif src_attr_datatype in ["string"] and tgt_field_type in ["int", "integer", "int4"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2int4({src_attr})"

        elif src_attr_datatype in ["string"] and tgt_field_type in ["bigint", "int8"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2int8({src_attr})"

        elif src_attr_datatype in ["string"] and tgt_field_type in ["bool", "boolean"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2bool({src_attr})"

        elif src_attr_datatype in ["string"] and tgt_field_type in ["decimal"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2decimal({src_attr})"

        else:
            value_type = "column"
            value = src_attr

        fld = MartField(tgt_field=tgt_field, value_type = value_type, value=value,
                        tgt_field_type=tgt_field_type.upper(),expression=expression)
        return fld

class MartHub:
    def __init__(self, hub_target: str, rk_field: str, business_key_schema: str, on_full_null: str, src_attribute: str,
                 src_type: str, field_type: str, is_bk: bool, schema: str, expression: str):

        self.schema = schema            # Схема в базе данных, где расположена хаб-таблица
        self.hub_target = hub_target    # Имя хаб-таблицы без схемы
        self.table = hub_target
        self.hub_name_only = self.hub_target
        self.full_table_name = self.schema + '.' + self.hub_target
        self.resource_cd = "ceh." + self.schema + '.' + self.hub_target
        self.short_name = create_short_name(name=self.hub_target, short_name_len=22, random_str_len=6)

        self.rk_field = rk_field
        self.id_field = self.rk_field.removesuffix('_rk') + '_id'

        self.business_key_schema = business_key_schema
        self.bk_schema_name = business_key_schema
        self.on_full_null = on_full_null
        self.src_attribute = src_attribute
        self.expression = expression if type(expression) is str else ''
        self.src_type = src_type
        self.field_type = field_type
        self.is_bk = 'true' if is_bk else 'false'
        self.src_cd = ''
        self.actual_dttm_name = ''

        # Разбираемся со значениями
        if self.expression:
            if self.src_type.upper() == 'STRING':
                self.expression = f"case when {self.expression} = '' then Null else {self.expression} end"
        else:
            if self.src_type.upper() == 'STRING':
                self.expression = f"case when {src_attribute} = '' then Null else {src_attribute} end"


class Mart:
    # Список полей целевой таблицы, которые не будут добавлены в секцию field_map шаблона flow.wk.yaml
    _ignore_field_map_ctx_list: dict

    def __init__(self, short_name: str, algorithm_uid: str, algorithm_uid_2: str, target: str, source: str,
                 delta_mode: str, processed_dt: str, algo: str, source_system: str, source_schema: str, source_name: str,
                 table_name: str,
                 actual_dttm_name: str, src_cd: str, comment: str):

        Mart._ignore_field_map_ctx_list = Config.setting_up_field_lists.get('ignore_field_map_ctx_list', dict())

        # Список полей mart-таблицы
        self.fields: [MartField] = []
        #  Список hub - таблиц, связанных с mart
        self.mart_hub_list: [MartHub] = []

        self.short_name = short_name
        self.algorithm_uid = algorithm_uid
        self.algorithm_uid_2 = algorithm_uid_2
        self.target = target
        self.table_name = table_name
        self.source = source
        self.delta_mode = delta_mode
        self.processed_dt = processed_dt
        self.algo = algo
        self.source_system = source_system.lower()
        self.source_schema = source_schema.lower()
        self.source_name = source_name.lower()
        self.actual_dttm_name = actual_dttm_name.lower()
        self.src_cd = src_cd
        self.comment = comment


        # Список полей с описанием, которые БУДУТ добавлены в секцию field_map шаблона flow.wk.yaml
        add_field_map_ctx_lis: dict = Config.setting_up_field_lists.get('add_field_map_ctx_list', dict())
        if type(add_field_map_ctx_lis) is dict:
            tgt_field_keys = add_field_map_ctx_lis.keys()
            for tgt_field in tgt_field_keys:
                # Описание поля
                fld = add_field_map_ctx_lis[tgt_field]
                mart_field = MartField(tgt_field=tgt_field, value_type=fld.type, value=fld.value,
                                       tgt_field_type=fld.field_type.upper(), expression="")
                self.add_fields(mart_field)

    def add_fields(self, mart_field: MartField):

        #  Не добавляем поля из списка "ignore_field_map_ctx_list"
        if mart_field.tgt_field in Mart._ignore_field_map_ctx_list:
            return

        # Проверяем, если поле уже присутствует, то выдается ошибка
        if [ctx.tgt_field for ctx in self.fields if ctx.tgt_field == mart_field.tgt_field]:
            msg = f'Поле "{mart_field.tgt_field}" уже присутствует в списке полей'
            raise IncorrectMappingException(msg)

        # Ставим признак, того, что поле не надо выводить в секцию field_map
        for i in range(len(self.mart_hub_list)):
            if self.mart_hub_list[i].rk_field == mart_field.tgt_field:
                 mart_field.is_hub_field = True

        self.fields.append(mart_field)

    def add_mart_hub_list(self, mart_hub: MartHub):
        mart_hub.actual_dttm_name = self.actual_dttm_name
        mart_hub.src_cd = self.src_cd

        # Ставим признак, того, что поле не надо выводить в секцию field_map
        for i in range(len(self.fields)):
            if self.fields[i].tgt_field == mart_hub.rk_field:
                self.fields[i].is_hub_field = True

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

        self.created = f'"{datetime.now().strftime("%d %b %Y %H:%M:%S")}"'


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
            if not [True for ctx in self.hubs if ctx.full_table_name == hub.full_table_name]:
                self.hubs.append(hub)


    def tags_formation(self):

        cfg_tags = Config.resource_tags
        if type(cfg_tags) is list:
            for tag in cfg_tags:
                if type(tag) is dict:
                    self.resource_tags.append("'" + list(tag.keys())[0] + ':' + list(tag.values())[0] + "'")
                else:
                    self.resource_tags.append("'" + tag + "'")

        # Добавляем строки из файла конфигурации
        cfg_tags = Config.tags
        if type(cfg_tags) is list:
            for tag in cfg_tags:
                if type(tag) is dict:
                    self.tags.append("'" + list(tag.keys())[0] + ':' + list(tag.values())[0] + "'")
                else:
                    self.tags.append("'" + tag + "'")

        # Добавляем динамические строки
        self.tags.append("'prv:" + self.sources[0].system + "'")
        self.tags.append("'src:" + self.targets[0].src_cd.upper() + "'")
        self.tags.append("'tgt:" + self.targets[0].schema + "'")
        self.tags.append("'cf_" + self.base_flow_name + "'")
        self.tags.append("'wf_" + self.base_flow_name + "'")

        # Источники
        for src in self.sources:
            self.tags.append("'src_tbl:" + src.schema + "." + src.table + "'")
        # Алгоритм
        for src in self.sources:
            self.tags.append("'UID:" + src.algorithm_uid + "'")

        # Целевая таблица
        for tgt in self.targets:
            if tgt.table.startswith("'mart_"):
                self.tags.append("'tgt_tbl:" + tgt.table + "'")


    def add_target_table(self, target_table: TargetTable):
        target_table.multi_fields.sort()
        target_table.hash_fields.sort()

        self.target_tables.append(target_table)
