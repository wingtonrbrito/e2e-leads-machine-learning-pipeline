"""
$dag_filename$: bq_to_wrench.py
"""
import os
import uuid
from libs import tempdir
from libs import GCLOUD as gcloud, CloudStorage
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import GetCheckpointOperator
from airflow.operators.custom_operators import SetCheckpointOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
import logging
from libs import BigQuery
from libs import report_failure
from libs.cleanup import cleanup_xcom
import requests
from libs import WrenchAuthWrapper, WrenchUploader

DAG_ID = 'bq_to_wrench'
env = os.environ['ENV']
project = gcloud.project(env)
gcs_bucket = f'{project}-leadengine-exports'
gcs_processed_bucket = f'{gcs_bucket}-processed'
gcs_failed_bucket = f'{gcs_bucket}-failed'

log = logging.getLogger()


default_args = {
    'owner': 'airflow',
    'description': 'Offload data from BQ to GCS; Upload GCS files to Leadengine /upload endpoint.',  # noqa: E501
    'start_date': datetime(2024, 3, 27),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


tables = [
    {
        'table': 'staging.zleads',
        'select': [
            'client', 'first_name', 'last_name', 'email', 'email2', 'email3',
            'phone', 'phone2', 'phone3', 'twitter', 'city', 'state', 'zip', 'country', 'gender', 'age'
        ]
    },
    {
        'table': 'staging.contacts',
        'select': [
            'client', 'first_name', 'last_name', 'email', 'email2', 'email3',
            'phone', 'phone2', 'phone3', 'twitter', 'city', 'state', 'zip', 'country'
        ]
    }
]


def insert_status_record(client, status, table, file, datetime_now):
    bq_client = BigQuery(env)
    query = build_status_insert(client, status, table, file, datetime_now)
    bq_client.query(query)


def build_status_insert(client, status, table, file, start_date):
    return BigQuery.querybuilder(
        table='leadengine.data_source_status',
        insert=[
            {'client': client, 'file': file, 'table': table,
             'status': status, 'start_date': start_date}]
        )


def _move_failed_file(storage_client, source_bucket, blob):
    log.info(f'Moving {blob.name} from {gcs_bucket} to {gcs_failed_bucket}')
    storage_client.move_files(source_bucket, storage_client.get_bucket(gcs_failed_bucket), prefix=blob.name)


def _is_cloud_storage_dir(object_name):
    return object_name.endswith('/')


def offload_to_wrench(env, table):
    # This tmp directory should be removed once the operation is complete,
    # because of GDPR. Stu M. 11/29/19
    with tempdir() as tmp:
        storage_client = CloudStorage.factory(project)
        bucket = storage_client.get_bucket(gcs_bucket)

        blobs = bucket.list_blobs(prefix=table)
        for blob in blobs:
            key = blob.name
            file_or_dir = '{}/{}'.format(tmp, key)
            if _is_cloud_storage_dir(key):
                os.mkdir(file_or_dir)
            else:
                dirname = os.path.dirname(file_or_dir)
                if not os.path.isdir(dirname):
                    os.mkdir(dirname)
                blob.download_to_filename(file_or_dir)

                # Derive client name from the blob name
                client = blob.name.split('-')[1]
                api_key = WrenchAuthWrapper.instance(env).get_api_key(client)

                log.info(f'Uploading {key} to Leadengine')
                response = WrenchUploader.upload(api_key, file_or_dir, key.replace('.gz', ''))

                if (response.status_code != requests.codes.ok):
                    try:
                        _move_failed_file(storage_client, bucket, blob)
                    except Exception as e:
                        log.error(f'There was an error moving the failed file: {e}')
                    finally:
                        raise Exception(f'''Error pushing {key} to Leadengine.
                                            Status: {response.status_code};
                                            Body: {response.text}''')

                log.info(response.text)

                if (response.json().get('file', None) is None):
                    try:
                        _move_failed_file(storage_client, bucket, blob)
                    except Exception as e:
                        log.error(f'There was an error moving the failed file: {e}')
                    finally:
                        raise Exception('Leadengine response did not contain the "file" attribute')

                processed_bucket = storage_client.get_bucket(gcs_processed_bucket)
                storage_client.move_files(bucket, processed_bucket, prefix=key)
                file = response.json()['file']
                insert_status_record(client, 'running', table, file, get_datetime_now())


def get_datetime_now():
    return str(datetime.utcnow())


def continue_if_data(table, **kwargs):
    checkpoint = kwargs['ti'].xcom_pull(key=table)
    if isinstance(checkpoint, dict) and checkpoint['has_data'] is True:
        return 'parse_query_{}'.format(table)
    else:
        return 'finish'


def build_query(checkpoint=None, tree_user_ids=None, project=None, table=None):

    if table is not None:
        filtered_table = list(filter(lambda t: t['table'] == table, tables))
        if len(filtered_table) == 0:
            raise SyntaxError(f'table: {table} not found in list')
        else:
            filtered_table = filtered_table[0]

    wheres = []
    if checkpoint is not None:
        # Break up query string so it passes linting. - Stu M 5/21/20
        f = checkpoint['first_ingestion_timestamp']
        last = checkpoint['last_ingestion_timestamp']
        wheres.append(
            f"ingestion_timestamp BETWEEN '{f}' AND '{last}'"
        )

    # unnested not being used, but we might need this soon. - Stu M. 8/19/20
    unnested = filtered_table.get('unnested', None)
    if unnested is None:
        _from = f'`{project}.{table}`'
    else:
        _from = f'`{project}.{table}`, {unnested}'

    select = filtered_table.get('select', '*')
    if len(wheres) > 0:
        return BigQuery.querybuilder(
            select=select,
            where=wheres,
            from_=_from
        )
    else:
        return BigQuery.querybuilder(
            select=select,
            from_=_from
        )


def parse_query(table, **kwargs):
    querybuilder = build_query(
        checkpoint=kwargs['ti'].xcom_pull(key=table),
        project=project,
        table=table)
    return str(querybuilder)


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              schedule_interval='30 * * * *',
              catchup=False,
              on_success_callback=cleanup_xcom)
    with dag:
        start_task = DummyOperator(
            task_id='start'
        )

        finish_task = DummyOperator(
            task_id='finish'
        )

        for table in tables:
            table = table['table']

            get_checkpoint_task = GetCheckpointOperator(
                task_id='get_checkpoint_{}'.format(table),
                env=env,
                target=table,
                sources=[table]
            )

            continue_if_data_task = BranchPythonOperator(
                task_id='continue_if_data_{}'.format(table),
                python_callable=continue_if_data,
                op_args=[table],
                provide_context=True
            )

            parse_query_task = PythonOperator(
                task_id=f'parse_query_{table}',
                python_callable=parse_query,
                op_args=[table],
                provide_context=True
            )

            dataflow_task = ScheduleDataflowJobOperator(
                task_id=f'schedule_dataflow_{table}',
                project=gcloud.project(env),
                template_name='offload_bq_to_cs',
                job_name=f'bq-to-leadengine-{table}',
                job_parameters={
                    'bucket': gcs_bucket,
                    'table': table,
                    'file': f'{uuid.uuid4()}.csv.gz'
                },
                pull_parameters=[{
                    'param_name': 'query',
                    'task_id': f'parse_query_{table}'
                }],
                provide_context=True
            )

            monitor_dataflow_task = DataflowJobStateSensor(
                task_id=f'monitor_dataflow_{table}',
                pusher_task_id=f'schedule_dataflow_{table}',
                dag=dag
            )

            offload_to_wrench_task = PythonOperator(
                task_id=f'offload_to_wrench_{table}',
                python_callable=offload_to_wrench,
                op_args=[env, table]
            )

            commit_checkpoint_task = SetCheckpointOperator(
                task_id=f'commit_checkpoint_{table}',
                env=env,
                table=table
            )

            (
                start_task
                >> get_checkpoint_task
                >> continue_if_data_task
                >> parse_query_task
                >> dataflow_task
                >> monitor_dataflow_task
                >> offload_to_wrench_task
                >> commit_checkpoint_task
                >> finish_task
            )
    return dag


globals()[DAG_ID] = create_dag()
