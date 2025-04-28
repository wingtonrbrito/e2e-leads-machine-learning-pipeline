from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from libs import CloudStorage
from libs import GoogleCloudServiceFactory
from airflow.models import Variable


class DataflowJobStateSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, pusher_task_id, *args, **kwargs):
        super(DataflowJobStateSensor, self).__init__(*args, **kwargs)
        self._service = GoogleCloudServiceFactory.build('dataflow')
        self._pusher_task_id = pusher_task_id
        airflow_vars = Variable.get('airflow_vars', deserialize_json=True)

        if kwargs['dag'] is None:
            raise Exception('Dag reference required.')

        dag_id = kwargs['dag'].dag_id
        dags = airflow_vars.get('dags', {})
        table = dags.get(dag_id, {})
        sensors = airflow_vars.get('sensors', {})
        dataflow_job_sensor = sensors.get('dataflow_job_state_sensor', {})
        table_poke_interval = table.get('poke_interval', None)
        if table_poke_interval:
            self.poke_interval = table_poke_interval
            self.timeout = table.get('poke_timeout', None)
        else:
            self.poke_interval = dataflow_job_sensor.get('poke_interval', None)
            self.timeout = dataflow_job_sensor.get('poke_timeout', None)
        self.mode = 'reschedule'

    def poke(self, context):
        print('#######################################')
        print('task_id: {}'.format(context['ti'].task_id))
        dataflow_job = context['ti'].xcom_pull(task_ids=self._pusher_task_id)
        print('job_id: {}'.format(dataflow_job['id']))
        print('#######################################')

        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs?authuser=1#Job.JobState jc 2/4/2020
        terminal_states = ['JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_UPDATED', 'JOB_STATE_DRAINED']

        state = None
        response = {}
        try:
            request = self._service.projects().locations().jobs().get(
                projectId=dataflow_job['projectId'],
                location=dataflow_job['location'],
                jobId=dataflow_job['id']
            )

            # state = 'JOB_STATE_DONE'
            response = request.execute()
            state = response['currentState']
        except Exception:
            pass

        if state in terminal_states:
            ex_error = 'The Dataflow job id {} was terminal'.format(dataflow_job['id'])
            raise Exception(ex_error)
        elif state == 'JOB_STATE_DONE':
            return True
        else:
            return False


class GCSFileUploadSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, project_id, bucket, file_name, *args, **kwargs):
        super(GCSFileUploadSensor, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.bucket = bucket
        self.file_name = file_name
        self.mode = 'reschedule'

    def execute(self, context):
        state = CloudStorage.factory(self.project_id).blob_exists(
            self.bucket,
            self.file_name
        )
        return state


class CustomPlugin(AirflowPlugin):
    name = "custom_sensors"
    sensors = [DataflowJobStateSensor, GCSFileUploadSensor]
