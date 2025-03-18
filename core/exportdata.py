import os

from jinja2 import Environment, FileSystemLoader

from core.config import Config
from core.flowcontext import FlowContext


class ExportData:
    templates_path: str
    flow_context: FlowContext | None
    env: Environment

    def __init__(self, templates_path: str, path: str):
        self.path = path

        self.templates_path = templates_path
        self.flow_context = None
        self.env = Environment(loader=FileSystemLoader(Config.templates_path))

    def generate_files(self):

        # Файл потока wf_*.yaml ----------------------------------------------------------------------------------------
        exp_path = os.path.join(self.path, "ceh-etl\\general_ledger\\src_rdv\\schema\\work_flows")
        file_path = os.path.join(exp_path, self.flow_context.flow_name + '.yaml')
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('wf.yaml')
        output = template.render(ctx=self.flow_context)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output)

        # py - файл потока управления cf_*.yaml ------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\flow_dumps")
        file_path = os.path.join(exp_path, "cf_" + self.flow_context.base_flow_name + '.yaml')
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('cf.yaml')
        output = template.render(ctx=self.flow_context)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output)


        # py - файл рабочего потока (wf_*.py)
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\dags")
        file_path = os.path.join(exp_path, self.flow_context.flow_name + '.py')
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('wf.py')
        output = template.render(ctx=self.flow_context)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output)


        # uni - ресурсы (*.json)
        exp_path = os.path.join(self.path, r"ceh-etl\_resources\uni")
        os.makedirs(exp_path, exist_ok=True)
        for uni in self.flow_context.sources:
            file_path = os.path.join(exp_path, uni.file_name)
            template = self.env.get_template('uni_res.json')
            output = template.render(ctx=uni, tags=self.flow_context.resource_tags)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Скрипт создания mart-таблиц
        exp_path = os.path.join(self.path, r"ceh-ddl\extensions\ripper\.data")
        os.makedirs(exp_path, exist_ok=True)
        for mart in self.flow_context.target_tables:
            if mart.table_type == 'MART':
                file_path = os.path.join(exp_path, mart.file_name + '.sql')
                template = self.env.get_template('mart_ddl.sql')
                output = template.render(ctx=mart)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(output)


        # Файл описания mart-таблицы
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\schema\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        for mart in self.flow_context.target_tables:
            if mart.table_type == 'MART':
                file_path = os.path.join(exp_path, mart.table_name + '.yaml')
                template = self.env.get_template('mart.yaml')
                output = template.render(ctx=mart)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(output)


        # Ресурсы целевых mart-таблицы
        exp_path = os.path.join(self.path, r"ceh-etl\_resources\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('ceh_res.json')

        for mart in self.flow_context.target_tables:
            if mart.table_type == 'MART':
                file_path = os.path.join(exp_path, 'ceh.' + mart.schema + '.' + mart.table_name + '.yaml')
                output = template.render(ctx=mart)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Необязательные скрипты создания/заполнения hub - таблиц
        exp_path = os.path.join(self.path, r"src\hub")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('hub_create.sql')
        for hub in self.flow_context.hubs:
            file_path = os.path.join(exp_path, hub.full_table_name + '.sql')
            output = template.render(ctx=hub)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Описание хаб - таблиц (hub_*.yaml)
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\schema\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('hub.yaml')
        for hub in self.flow_context.hubs:
            file_path = os.path.join(exp_path, hub.hub_name_only + '.yaml')
            output = template.render(ctx=hub)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)

        # Ресурсы хабов
        exp_path = os.path.join(self.path, r"ceh-etl\_resources\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('ceh_bk_schema.json')
        for hub in self.flow_context.hubs:
            file_path = os.path.join(exp_path, 'ceh.' + hub.full_table_name + hub.business_key_schema + '.json')
            output = template.render(ctx=hub, tags=self.flow_context.resource_tags)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        #  Скрипт формирования акцессоров для mart-таблиц
        exp_path = os.path.join(self.path, r"src")
        os.makedirs(exp_path, exist_ok=True)
        for mart in self.flow_context.target_tables:
            if mart.table_type == 'MART':
                file_path = os.path.join(exp_path, 'acc.' + mart.file_name + '.sql')
                template = self.env.get_template('f_gen_access_view.sql')
                output = template.render(ctx=mart)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(output)


        # Описание внешних таблиц-источников
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\schema\db_tables")
        os.makedirs(exp_path, exist_ok=True)
        for src in self.flow_context.sources:
            file_path = os.path.join(exp_path, src.table + '.yaml')
            template = self.env.get_template('db_table.yaml')
            output = template.render(ctx=src)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)



