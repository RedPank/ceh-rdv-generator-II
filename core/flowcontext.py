import copy
import os
import random
import string
from datetime import datetime

from pandas import Series

from core.config import Config
from core.exceptions import IncorrectMappingException


def actual_date_name(src_cd: str)->str:
    return src_cd.lower() + '_actual_dttm'

def create_short_name(name: str, short_name_len: int, random_str_len: int,
                      char_set: str = string.ascii_lowercase + string.digits,
                      always_expand_name: bool = False):
    """
    Функция формирует "короткое имя" на основе значения в переменной name
    Если длина name меньше чем short_name_len, то ф-ия возвращает name без преобразования.
    Если длина name больше чем short_name_len, то name усекается до длинны (short_name_len - random_str_len) и
    дополняется рандомной строкой до длинны short_name_len.
    pattern=^[a-z][a-z0-9_]{2,22}$
    Args:
        name: Имя, на основе которого надо сформировать "короткое имя".
        short_name_len: Длина "короткого имени", которое надо сформировать.
        random_str_len: Длина рандомной строки, которая используется для формирования "короткого имени".
        char_set: Набор символов, на основе которого формируется "рандомная" строка.
        always_expand_name: Если expand_name is True то короткое имя всегда формируется с использованием "рандомной" строки.

    Returns: Строка "Короткое имя"
    """

    short_name: str

    if len(name) > short_name_len or always_expand_name:
        short_name = name[0:short_name_len - random_str_len]
        short_name = (short_name + ''.join(random.choice(char_set) for _ in range(random_str_len)))
    else:
        short_name = name

    return short_name.lower()


# Класс TargetTable ----------------------------------------------------------------------------------------------------
class DataBaseField:

    def __init__(self, name: str, data_type: str, comment:str, is_nullable: bool, is_pk, properties = None):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable
        self.comment = comment

        if properties is None:
            self.properties = dict()
        else:
            self.properties = properties

        if type(is_pk) is bool:
            self.is_pk = is_pk
        elif type(is_pk) is str:
            self.is_pk = str(is_pk).strip().lower() == 'pk'
        else:
            self.is_pk = False

class TargetTable:

    _ignore_primary_key = None
    _ignore_hash_fields = None
    _ignore_multi_fields = None

    def __init__(self, schema: str, table_name: str, comment: str, table_type: str, src_cd: str,
                 distribution_field: str):

        if not TargetTable._ignore_primary_key:
            TargetTable._ignore_primary_key = Config.setting_up_field_lists.get('ignore_primary_key', list())
        if not TargetTable._ignore_hash_fields:
            TargetTable._ignore_hash_fields = Config.setting_up_field_lists.get('ignore_hash_set', list())
        if not TargetTable._ignore_multi_fields:
            TargetTable._ignore_multi_fields = Config.setting_up_field_lists.get('ignore_multi_fields', list())


        self.fields: list[DataBaseField] = []
        self.resource_ceh_fields = []
        self.hash_fields = []
        self.multi_fields = []
        self.hub_fields = []

        self.distributed_by = ''
        self.primary_key = ''

        self.schema = schema
        self.table_name = table_name
        self.comment = comment
        self.table_type = table_type.upper()
        self.src_cd = src_cd
        self.actual_dttm_name = actual_date_name(self.src_cd)

        self.file_name = '.'.join([self.schema, self.table_name]).lower()
        self.distribution_field= distribution_field.lower()
        self.distributed_by = self.distribution_field

        self.ceh_aliases: dict = Config.field_type_list.get('ceh_datatype_aliases', dict())


    def add_field(self, field: DataBaseField):
        self.fields.append(field)

        # Список полей для описания ресурса.
        # Допустимые типы полей в разных файлах потока - разные. :-()
        ceh_field: DataBaseField = copy.deepcopy(field)
        if field.data_type in self.ceh_aliases:
            ceh_field: DataBaseField = copy.deepcopy(field)
            ceh_field.data_type = self.ceh_aliases[field.data_type]

        self.resource_ceh_fields.append(ceh_field)

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
        if field.is_pk and not field.properties.get("is_hub_field", False) and field.name not in TargetTable._ignore_multi_fields:
            self.multi_fields.append(field.name)

    def add_hub_field(self, hub: 'HubMartField'):
        self.hub_fields.append(hub)

    # Выставляем на первое место поля с признаком not null.
    # Это "заморочка", что-бы отработал Ripper, который циклится на списке полей в таблице из-за
    # некорректного регулярного выражения. А так - работает. :-)
    def fields_sort(self):
        self.fields.sort(key = lambda obj: ('A' if obj.is_pk else 'B') + obj.name)


class Source:

    def __init__(self, system: str, schema: str, table: str, algorithm_uid:str, algorithm_uid_2: str,
                 ceh_resource: str, src_cd: str, data_capture_mode: str):
        self.system = system
        self.source_system = self.system

        self.schema = schema
        self.table = table.upper()
        self.algorithm_uid=algorithm_uid
        self.algorithm_uid_2=algorithm_uid_2
        self.src_cd = src_cd
        self.data_capture_mode = data_capture_mode

        # Длина short_name должна быть от 2 до 22 символов
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)

        # Для разных схем формирование имени может различаться
        uni_resource_template = Config.config.get("uni_resource_template", None)
        if uni_resource_template is None:
            self.uni_res = self.system.lower() + '.' + self.schema.lower() + '.' + self.table.lower()
        else:
            # До момента вызова этой строки все необходимые переменные должны быть определены!
            template = Config.env.from_string(uni_resource_template)
            self.uni_res = template.render(uni=self)

        self.resource_cd = self.uni_res

        self.instance = (self.system + '_' + self.schema).lower()
        self.actual_dttm_name =  actual_date_name(self.src_cd)

        self.ceh_res = ceh_resource

        self.file_name = '.'.join([self.system, self.schema, self.table, 'json']).lower()
        self.fields = []

    def add_field(self, field: DataBaseField):
        self.fields.append(field)

class Target:

    def __init__(self, schema: str, table: str, src_cd: str, object_type: str,
                 uni_resource_cd: str, resource_cd: str = None):

        self.schema = schema
        self.table = table
        self.short_name = create_short_name(name=self.table, short_name_len=22, random_str_len=6)
        self.src_cd = src_cd
        self.object_type = object_type
        self.resource_cd = resource_cd if resource_cd is not None else '.'.join(['ceh', self.schema, self.table])
        self.uni_resource_cd = uni_resource_cd


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

    def __init__(self, tgt_field: str, value_type: str, value: str, expression: str, tgt_field_type: str, is_hub_field = False):
        self.tgt_field = tgt_field
        self.value_type = value_type
        self.value = value
        self.expression = expression
        self.tgt_field_type = tgt_field_type
        self.is_hub_field = is_hub_field

    @staticmethod
    def create_mart_field(row: Series):

        src_attr: str = str(row["src_attribute"]).strip().lower()
        src_attr_datatype:str = str(row["src_attr_datatype"]).strip().lower()
        tgt_field:str = str(row["tgt_attribute"]).strip().lower()
        tgt_field_type:str = str(row["tgt_attr_datatype"]).strip().lower()
        expression: str = str(row["expression"]).strip().removeprefix('=')
        is_pk = row["is_pk"]

        is_hub_field: bool = False
        if row['attr:conversion_type'] == 'hub':
            is_hub_field = True

        value = src_attr

        if expression:
            value_type = "sql_expression"
            expression = expression + ' :: ' + tgt_field_type

        elif src_attr_datatype in ["text"] and tgt_field_type in ["text"] and not is_pk:
            value_type = "sql_expression"
            expression = f"case when {src_attr} = '' then Null else {src_attr} end"  + ' :: ' + tgt_field_type

        elif src_attr_datatype in ["text"] and tgt_field_type in ["timestamp"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2ts({src_attr})"

        elif src_attr_datatype in ["text"] and tgt_field_type in ["date"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2dt({src_attr})"

        elif src_attr_datatype in ["text"] and tgt_field_type in ["smallint", "int2"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2int2({src_attr})"

        elif src_attr_datatype in ["text"] and tgt_field_type in ["int", "integer", "int4"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2int4({src_attr})"

        elif src_attr_datatype in ["text"] and tgt_field_type in ["bigint", "int8"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2int8({src_attr})"

        elif src_attr_datatype in ["text"] and tgt_field_type in ["bool", "boolean"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2bool({src_attr})"

        elif src_attr_datatype in ["text"] and tgt_field_type in ["decimal"]:
            value_type = "sql_expression"
            expression = f"etl.try_cast2decimal({src_attr})"

        else:
            value_type = "column"
            value = src_attr


        fld = MartField(tgt_field=tgt_field, value_type = value_type, value=value,
                        tgt_field_type=tgt_field_type,expression=expression, is_hub_field=is_hub_field)
        return fld

class HubMartField:
    def __init__(self, hub_table: str, rk_field: str, business_key_schema: str, on_full_null: str, src_attribute: str,
                 src_type: str, field_type: str, is_bk: bool, schema: str, expression: str, mart_retain_key:str):

        self.schema = schema            # Схема в базе данных, где расположена хаб-таблица

        self.hub_table = hub_table              # Имя хаб-таблицы без схемы
        self.table = hub_table                  # Имя хаб-таблицы без схемы
        self.hub_name_only = hub_table          # Имя хаб-таблицы без схемы

        self.full_table_name = self.schema + '.' + self.hub_table
        self.resource_cd = "ceh." + self.schema + '.' + self.hub_table
        self.short_name = create_short_name(name=self.hub_table, short_name_len=22, random_str_len=6)

        # Название поля хаба, в котором хранится retain key
        self.rk_field = rk_field
        self.id_field = self.rk_field.removesuffix('_rk') + '_id'

        # Название поля в марте, в которое будет записываться значение retain key хаба
        self.mart_retain_key = mart_retain_key

        self.business_key_schema = business_key_schema
        self.bk_schema_name = business_key_schema
        self.on_full_null = on_full_null
        self.src_attribute = src_attribute
        self.expression = expression if type(expression) is str else ''
        self.src_type = src_type
        self.field_type = field_type
        self.is_bk = is_bk
        self.src_cd = ''
        self.actual_dttm_name = ''

        # Разбираемся со значениями
        if self.expression:
            if self.src_type.upper() == 'TEXT':
                self.expression = f"case when {self.expression} = '' then null else {self.expression} end"
        else:
            if self.src_type.upper() == 'TEXT':
                self.expression = f"case when {src_attribute} = '' then null else {src_attribute} end"


class Mart:
    # Список полей целевой таблицы, которые не будут добавлены в секцию field_map шаблона flow.wk.yaml
    _ignore_field_map_ctx_list: dict

    def __init__(self, short_name: str, algorithm_uid: str, algorithm_uid_2: str, target: str, source: str,
                 delta_mode: str, processed_dt: str, algo: str, source_system: str, source_schema: str, source_name: str,
                 table_name: str,
                 src_cd: str, comment: str, uni_resource_cd: str):

        Mart._ignore_field_map_ctx_list = Config.setting_up_field_lists.get('ignore_field_map_ctx_list', dict())

        # Список полей mart-таблицы
        self.fields = []
        #  Список hub - таблиц, связанных с mart
        self.mart_hub_list = []

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
        self.src_cd = src_cd
        self.actual_dttm_name = actual_date_name(self.src_cd)
        self.comment = comment
        self.uni_resource_cd = uni_resource_cd


        # Список полей с описанием, которые БУДУТ добавлены в секцию field_map шаблона flow.wk.yaml
        add_field_map_ctx_lis: dict = Config.setting_up_field_lists.get('add_field_map_ctx_list', dict())
        if type(add_field_map_ctx_lis) is dict:
            tgt_field_keys = add_field_map_ctx_lis.keys()
            for tgt_field in tgt_field_keys:
                # Описание поля
                fld = add_field_map_ctx_lis[tgt_field]
                mart_field = MartField(tgt_field=tgt_field, value_type=fld.type, value=fld.value,
                                       tgt_field_type=fld.field_type, expression="")
                self.add_fields(mart_field)

    def add_fields(self, mart_field: MartField):

        #  Не добавляем поля из списка "ignore_field_map_ctx_list"
        if mart_field.tgt_field in Mart._ignore_field_map_ctx_list:
            return

        # Проверяем, если поле уже присутствует, то выдается ошибка
        if [ctx.tgt_field for ctx in self.fields if ctx.tgt_field == mart_field.tgt_field]:
            msg = f'Поле "{mart_field.tgt_field}" уже присутствует в списке полей'
            raise IncorrectMappingException(msg)

        self.fields.append(mart_field)

    def add_mart_hub_list(self, mart_hub: HubMartField):
        mart_hub.actual_dttm_name = self.actual_dttm_name
        mart_hub.src_cd = self.src_cd

        self.mart_hub_list.append(mart_hub)


class FlowContext:

    author: str
    flow_name : str
    base_flow_name: str
    data_capture_mode: str
    delta_mode: str
    work_flow_schema_version: str
    # processed_dt_format: str


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

        self.processed_dt: str = Config.config.get('processed_dt', 'processed_dt_не_определено')
        self.processed_dt_conversion = Config.config.get('processed_dt_conversion', 'processed_dt_conversion_не_определено')

        self.tgt_history_field = Config.config.get('tgt_history_field', '')

        self.username = str(os.environ.get('USERNAME', 'Unknown Author')).title()
        self.author = Config.author
        self.created = f'"{datetime.now().strftime("%d %b %Y %H:%M:%S")}" by {self.author}'
        self.work_flow_schema_version = Config.work_flow_schema_version


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
                    self.resource_tags.append('"' + list(tag.keys())[0] + ':' + list(tag.values())[0] + '"')
                else:
                    self.resource_tags.append('"' + tag + '"')

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

        target_table.fields_sort()
        self.target_tables.append(target_table)
