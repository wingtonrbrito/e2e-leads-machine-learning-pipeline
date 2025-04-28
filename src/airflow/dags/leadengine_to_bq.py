"""
$dag_filename$: wrench_to_bq.py
"""
from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.dummy_operator import DummyOperator
import os
import logging
from datetime import datetime, timedelta
from libs import GCLOUD as gcloud
from libs import parse_template
from libs import BigQuery
from libs import QueryBuilder
from libs import report_failure
from libs.cleanup import cleanup_xcom
import json
from libs import WrenchAuthWrapper
import requests

log = logging.getLogger()
log.setLevel('INFO')
env = os.environ['ENV']
project_id = gcloud.project(env)
DAG_ID = 'wrench_to_bq'
airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
huf = airflow_vars['dags'][DAG_ID]['hours_until_filtered']
insight_ids = airflow_vars['dags'][DAG_ID]['insight_ids']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 11, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}


def get_running_files():
    bq_client = BigQuery(env)
    where = ["status = 'running'",
             "AND end_date is null",
             f"AND DATETIME_DIFF(current_datetime(), start_date, HOUR) <= {huf}"]
    query = BigQuery.querybuilder(
        select=['client', 'file', 'table'],
        where=[' '.join(where)],
        from_=f'{project_id}.leadengine.data_source_status'
        )
    return bq_client.query(query)


def import_entities():
    rs = get_running_files()
    for f in rs:
        job_status = fetch_job_status(f)
        status = job_status.get('status', 'UNKNOWN')

        if status == 'SUCCEEDED':
            entities = fetch_entities(f)
            if entities and len(entities) > 0:
                insert_entities(f, entities)

        if status != 'RUNNING':
            update_status(f, status)


def fetch_job_status(file_record):
    api_key = WrenchAuthWrapper.instance(env).get_api_key(file_record['client'])
    host = api_key['host']
    url = f'{host}/job-status'

    response = requests.post(
        url,
        data=json.dumps({
            'source_file': file_record['file']
        }),
        headers={
            'authorization': api_key['api_key'],
            'content_type': 'application/json'
        })

    if response.status_code == requests.codes.ok:
        log.info(f'Job Status response: status {response.status_code}; response: {response.text}')
        return response.json()
    else:
        raise Exception(f'''Error pulling job-status from Leadengine.
                            Status: {response.status_code};
                            Body: {response.text}''')


def fetch_entities(file_record):
    api_key = WrenchAuthWrapper.instance(env).get_api_key(file_record['client'])
    host = api_key['host']
    url = f'{host}/entity'

    response = requests.post(
        url,
        data=json.dumps({
            'source_file': file_record['file']
        }),
        headers={
            'authorization': api_key['api_key'],
            'content_type': 'application/json'
        })

    # TODO: refactor this logic after we're more familiar with the job-status results. jc 1/19/21
    if response.status_code == requests.codes.processing or response.status_code == requests.codes.no_content:
        log.info(f'Entity response: status {response.status_code}; return case 1')
        return
    elif response.status_code == requests.codes.bad and json.loads(response.text).get('error') == 'No Data Found':
        log.info(f'Entity response: status {response.status_code}; return case 2')
        return
    elif response.status_code == requests.codes.ok:
        log.info(f'Entity response: status {response.status_code}; response[:100]: {response.text[:100]}')
        return response.json()
    else:
        raise Exception(f'''Error pulling entities from Leadengine.
                            Status: {response.status_code};
                            Body: {response.text}''')


def insert_entities(file_record, entities):
    for e in entities:
        e['client'] = file_record['client']
        e['ingestion_timestamp'] = str(datetime.utcnow())
        e['file'] = file_record['file']

    bq_client = BigQuery(env)
    query = QueryBuilder(
        table='leadengine.entities',
        insert=entities
    )
    bq_client.query(query)


def update_status(file_record, new_status):
    bq_client = BigQuery(env)
    status_date = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    query = f"""UPDATE leadengine.data_source_status SET
                end_date = '{status_date}'
                WHERE file = '{file_record["file"]}'
                AND status = 'running'
                AND end_date IS NULL"""
    bq_client.query(query)
    query = f"""INSERT INTO leadengine.data_source_status(client, file, table, status, start_date) values(
            '{file_record['client']}', '{file_record["file"]}', '{file_record["table"]}',
            '{new_status.lower()}', '{status_date}')"""
    bq_client.query(query)


def should_continue(continue_task_id, destination_table):
    query = parse_template(f'{settings.DAGS_FOLDER}/templates/sql/wrench_to_bq.entities.sql',
                           **{'table': destination_table, 'hours': huf})
    bq_client = BigQuery(env)
    rs = bq_client.query(query)
    if len(rs) > 0:
        return continue_task_id
    return 'finish'


def create_dag():
    dag = DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval='45 * * * *',
        catchup=False,
        on_success_callback=cleanup_xcom,
        on_failure_callback=report_failure
    )

    with dag:
        start_task = DummyOperator(
            task_id='start'
        )
        import_entities_task = PythonOperator(
            task_id='import_entities',
            python_callable=import_entities
        )
        finish_task = DummyOperator(
            task_id='finish',
            trigger_rule='all_success'
        )
        for i in insight_ids:
            endpoint_name = i.replace('-', '_')
            destination_table = f'leadengine.{endpoint_name}'
            pusher_task_id = f'schedule_load_wrench_to_bq_{endpoint_name}'

            continue_task = BranchPythonOperator(
                task_id=f'continue_{endpoint_name}',
                python_callable=should_continue,
                op_args=[pusher_task_id, destination_table]
            )

            df_insights_task = ScheduleDataflowJobOperator(
                task_id=pusher_task_id,
                project=gcloud.project(env),
                template_name='load_wrench_to_bq',
                job_name=f'load_wrench_to_bq_{endpoint_name}',
                job_parameters={
                    'destination_table': destination_table,
                    'insight_id': i,
                    'entity_query': parse_template(f'{settings.DAGS_FOLDER}/templates/sql/wrench_to_bq.entities.sql',
                                                   **{'table': destination_table, 'hours': huf})
                }
            )
            monitor_df_job_task = DataflowJobStateSensor(
                task_id=f'monitor_load_wrench_to_bq_{endpoint_name}',
                pusher_task_id=pusher_task_id,
                dag=dag
            )
            (
                start_task
                >> import_entities_task
                >> continue_task
                >> df_insights_task
                >> monitor_df_job_task
                >> finish_task
            )
    return dag


globals()[DAG_ID] = create_dag()
