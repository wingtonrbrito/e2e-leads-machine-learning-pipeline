from airflow import settings
from airflow.models import Variable
import json


def get_airflow_vars():
    airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
    vars = {}
    with open(f'{settings.DAGS_FOLDER}/extended_variables.json', 'r') as f:
        file_json_content = f.read()
        vars = json.loads(file_json_content)
    airflow_vars = {**airflow_vars, **vars}
    return airflow_vars
