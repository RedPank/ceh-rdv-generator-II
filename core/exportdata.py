import os
from jinja2 import Environment, FileSystemLoader
from core.config import Config


class ExportData:

    def __init__(self, templates_path: str, path: str, flow_context):
        self.path = path

        self.templates_path = templates_path
        self.flow_context = flow_context
        self.env = Environment(loader=FileSystemLoader(Config.templates_path))

    def generate_files(self):

        # Файл потока wf_*.yaml ----------------------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\schema\work_flows")
        file_path = os.path.join(exp_path, self.flow_context.flow_name + '.yaml')
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('flow.wk.yaml')
        output = template.render(ctx=self.flow_context)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output)

        # py - файл потока управления cf_*.yaml ------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\flow_dumps")
        file_path = os.path.join(exp_path, "cf_" + self.flow_context.base_flow_name + '.yaml')
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('flow.cf.yaml')
        output = template.render(ctx=self.flow_context)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output)


        # py - файл рабочего потока (wf_*.py) --------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\dags")
        file_path = os.path.join(exp_path, self.flow_context.flow_name + '.py')
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('flow_wk.py')
        output = template.render(ctx=self.flow_context)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(output)


        # uni - ресурсы (*.json) ---------------------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\_resources\uni")
        os.makedirs(exp_path, exist_ok=True)
        for uni in self.flow_context.sources:
            file_path = os.path.join(exp_path, uni.file_name)
            template = self.env.get_template('resource.uni.table.json')
            output = template.render(ctx=uni, tags=self.flow_context.resource_tags)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Скрипт создания mart-таблиц ----------------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-ddl\extensions\ripper\.data")
        os.makedirs(exp_path, exist_ok=True)
        for target_table in self.flow_context.target_tables:
            if target_table.table_type == 'MART':
                file_path = os.path.join(exp_path, target_table.file_name + '.sql')
                template = self.env.get_template('create.table.mart.sql')
                output = template.render(ctx=self.flow_context, tgt=target_table)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(output)


        # Файл описания mart-таблицы -----------------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\schema\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('table.mart.yaml')
        for target_table in self.flow_context.target_tables:
            if target_table.table_type == 'MART':
                file_path = os.path.join(exp_path, target_table.table_name + '.yaml')
                output = template.render(ctx=self.flow_context, tgt=target_table)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(output)


        # Ресурсы целевых mart-таблицы ---------------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\_resources\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('resource.ceh.mart.json')
        for target_table in self.flow_context.target_tables:
            if target_table.table_type == 'MART':
                file_path = os.path.join(exp_path, 'ceh.' + target_table.schema + '.' + target_table.table_name + '.json')
                output = template.render(ctx=target_table)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Необязательные скрипты создания hub - таблиц -----------------------------------------------------------------
        exp_path = os.path.join(self.path, r"src\ceh-ddl\extensions\ripper\.data")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('create.table.hub.sql')
        for hub in self.flow_context.hubs:
            file_path = os.path.join(exp_path, hub.full_table_name + '.sql')
            output = template.render(ctx=hub)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Необязательные файлы - Описание хаб - таблиц (hub_*.yaml) ----------------------------------------------------
        exp_path = os.path.join(self.path, r"src\ceh-etl\general_ledger\src_rdv\schema\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('table.hub.yaml')
        for hub in self.flow_context.hubs:
            file_path = os.path.join(exp_path, hub.hub_name_only + '.yaml')
            output = template.render(ctx=self.flow_context, hub=hub)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)

        # Ресурсы хабов. Помещаются в каталог src ----------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"src\ceh-etl\_resources\ceh\rdv")
        os.makedirs(exp_path, exist_ok=True)
        template = self.env.get_template('resource.ceh.hub.bk_schema.json')
        template2 = self.env.get_template('resource.ceh.hub.json')
        for hub in self.flow_context.hubs:

            file_path = os.path.join(exp_path, 'ceh.' + hub.full_table_name + '.' + hub.business_key_schema + '.json')
            output = template.render(ctx=hub, tags=self.flow_context.resource_tags)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)

            file_path = os.path.join(exp_path, 'ceh.' + hub.full_table_name + '.json')
            output2 = template.render(ctx=hub, tags=self.flow_context.resource_tags)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)


        # Необязательные файлы - Скрипты формирования акцессоров для mart-таблиц ---------------------------------------
        exp_path = os.path.join(self.path, r"src")
        os.makedirs(exp_path, exist_ok=True)
        for target_table in self.flow_context.target_tables:
            if target_table.table_type == 'MART':
                file_path = os.path.join(exp_path, 'acc.' + target_table.file_name + '.sql')
                template = self.env.get_template('f_gen_access_view.sql')
                output = template.render(ctx=target_table)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(output)


        # Описание внешних таблиц-источников ---------------------------------------------------------------------------
        exp_path = os.path.join(self.path, r"ceh-etl\general_ledger\src_rdv\schema\db_tables")
        os.makedirs(exp_path, exist_ok=True)
        for src in self.flow_context.sources:
            file_path = os.path.join(exp_path, src.table + '.yaml')
            template = self.env.get_template('db_table.yaml')
            output = template.render(ctx=self.flow_context, src=src)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)



