# Created: {{ ctx.created }}
#  В рамках разработки потока {{ ctx.flow_name }}
{# Файл python - зачем-то нужен AF для работы потока #}

from pathlib import Path
from ceh_core_rdv.app import dag_builder

DAG = 'Нужна переменная DAG, чтобы airflow подгрузил файл'
dag = dag_builder.build(Path(__file__).stem)

if __name__ == '__main__':
    print(f'{dag.dag_id} loaded.')
