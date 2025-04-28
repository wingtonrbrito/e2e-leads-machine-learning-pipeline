"""
$dag_filename$: cdc_from_gcs_to_lake.py
"""
import os
import logging
from libs import GCLOUD as gcloud, CloudStorage
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from libs import report_failure
from libs.cleanup import cleanup_xcom

DAG_ID = 'cdc_from_gcs_to_lake'
log = logging.getLogger()
env = os.environ['ENV']
project_id = gcloud.project(env)
bucket = f'{project_id}-cdc-imports'
processed_bucket = f'{project_id}-cdc-imports-processed'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}

"""
Composer Webserver only handles dynamic tasks creation with variables
existing either in the DAG file or in the Airflow env which gets imported from
variables.json. In order to maximize memory resources,
place variables to generate dynamic tasks in the DAG file.
"""
table_map = {
    'vibe-zleads-final': 'zleads'
}


def should_continue(prefix=None, bucket=None, table=None):
    cdc_imports_bucket = CloudStorage.factory(project_id).get_bucket(bucket)
    if CloudStorage.factory(project_id).has_file(bucket=cdc_imports_bucket, prefix=prefix):
        return f'schedule_df_gcs_to_lake_{table}'
    else:
        return 'finish'


def move_files(files_startwith, cdc_imports_bucket, cdc_imports_processed_bucket):
    cdc_imports_bucket = CloudStorage.factory(project_id).get_bucket(cdc_imports_bucket)
    cdc_imports_processed_bucket = CloudStorage.factory(project_id).get_bucket(cdc_imports_processed_bucket)
    CloudStorage.factory(project_id).move_files(cdc_imports_bucket, cdc_imports_processed_bucket, prefix=files_startwith)


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              # Be sure to stagger the dags so they don't run all at once,
              # possibly causing max memory usage and pod failure. - Stu M.
              schedule_interval='0 * * * *',
              catchup=False,
              on_success_callback=cleanup_xcom)
    with dag:
        start_task = DummyOperator(task_id='start')
        finish_task = DummyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        for files_startwith, table in table_map.items():
            pusher_task_id = f'schedule_df_gcs_to_lake_{table}'
            continue_if_file_task = BranchPythonOperator(
                task_id=f'continue_if_file_{files_startwith}',
                python_callable=should_continue,
                op_args=[files_startwith, bucket, table]
            )
            schedule_df_task = ScheduleDataflowJobOperator(
                task_id=pusher_task_id,
                project=project_id,
                template_name='load_cdc_from_gcs_to_lake',
                job_name=f'gcs-to-lake-{table}',
                job_parameters={
                    'files_startwith': files_startwith,
                    'dest': f'{project_id}:lake.{table}'
                },
                provide_context=True
            )
            monitor_df_job_task = DataflowJobStateSensor(
                task_id=f'monitor_df_job_{table}',
                pusher_task_id=pusher_task_id,
                dag=dag
            )
            move_files_task = PythonOperator(
                task_id=f'move_processed_files_{files_startwith}',
                python_callable=move_files,
                op_args=[files_startwith, bucket, processed_bucket],
            )
            (
                start_task
                >> continue_if_file_task
                >> schedule_df_task
                >> monitor_df_job_task
                >> move_files_task
                >> finish_task
            )
    return dag


globals()[DAG_ID] = create_dag()
