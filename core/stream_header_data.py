import re
import pandas as pd


class StreamHeaderData:
    """
    Класс представляет данные с листа 'Перечень загрузок Src-RDV' для указанной целевой таблицы.
    Названия полей соответствуют названиям колонок EXCEL
    """

    def __init__(self, row: pd.Series):

        self.row = row
        self.version = self.row["version"]
        self.version_end = self.row["version_end"]
        self.algorithm_uid = re.sub(r"\s", '', self.row["algorithm_uid"])

        self.subalgorithm_uid = str(self.row["subalgorithm_uid"])
        # Отсекаем нули после точки/запятой. Из-за странностей EXCEL
        self.subalgorithm_uid = re.sub(r"([.,])0$", "", self.subalgorithm_uid)

        self.flow_name = re.sub(r"\s", '', self.row["flow_name"])

        self.tgt_full_name = re.sub(r"\s", '', self.row["tgt_table"])
        self.tgt_schema = self.tgt_full_name.split('.')[0]
        self.tgt_table = self.tgt_full_name.split('.')[1]
        self.tgt_resource_cd = 'ceh.' + self.tgt_full_name

        self.target_rdv_object_type = re.sub(r"\s", '', self.row["target_rdv_object_type"]).upper()

        # source_table
        self.src_full_name: str = re.sub(r"\s", '', self.row["src_table"])
        self.src_full_name = self.src_full_name.upper()

        # Делаем такой разбор, что-бы не было исключения в этом месте.
        # Проверка соответствия имени шаблону выполняется позже
        if self.src_full_name.find(".") > 0:
            self.src_schema = self.src_full_name.split('.')[0]
            self.src_table = self.src_full_name.split('.')[1]
        else:
            self.src_schema = ''
            self.src_table = self.src_full_name

        self.src_resource_cd = '???'

        self.source_system = re.sub(r"\s", '', self.row["source_name"]).upper()
        self.scd_type = re.sub(r"\s", '', self.row["scd_type"])
        self.distribution_field = self.row["distribution_field"]
        self.distribution_field = self.distribution_field.lower().strip()
        self.distribution_field = re.sub(r"\s", "", self.distribution_field)

        if type(self.distribution_field) is str:
            self.distribution_field_list = self.distribution_field.split(',')
            self.distribution_field_list.sort()
        else:
            self.distribution_field_list = []

        # Комментарий к таблице
        self.comment = self.row["comment"]

        # Имя потока без wf_/cf_
        self.base_flow_name = self.flow_name.removeprefix('wf_')

