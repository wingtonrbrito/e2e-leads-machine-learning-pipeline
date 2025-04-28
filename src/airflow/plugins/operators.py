import logging
import os
import dateutil.parser
from datetime import datetime, timedelta
from airflow.models import BaseOperator, SkipMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from libs import GoogleCloudServiceFactory
from libs import BigQuery
from googleapiclient.errors import HttpError

log = logging.getLogger()


class GetCheckpointOperator(BaseOperator):

    @apply_defaults
    def __init__(self, env, target, sources, *args, **kwargs):
        self._target = target
        self._sources = sources
        self._bq = BigQuery(env)
        super(GetCheckpointOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        checkpoint_query = f"""
            SELECT checkpoint
            FROM `system.checkpoint`
            WHERE `table`='{self._target}'
            AND dag_id = '{context['ti'].dag_id}'
        """

        rs = self._bq.query(checkpoint_query)

        xcom = {
            'table': self._target,
            'dag_id': context['ti'].dag_id,
            'has_data': False}

        if len(rs) < 1:
            xcom["first_ingestion_timestamp"] = '1970-01-01 00:00:00'
        else:
            xcom["first_ingestion_timestamp"] = str(rs[0]["checkpoint"])

        """
            Pre fetches the last eid and injestion timestamp - Stu M 2/14/20
        """
        union = []
        for i, t in enumerate(self._sources):
            union.append(BigQuery.querybuilder(
                from_=BigQuery.querybuilder(
                    from_=f'{t}',
                    select=['ingestion_timestamp', (f'"{t}"', 'tbl')],
                    where=[f'ingestion_timestamp >= "{xcom["first_ingestion_timestamp"]}"'],
                    order=[('ingestion_timestamp', 'asc')]
                ),
                select='*'))

        builder = BigQuery.querybuilder(
            from_=BigQuery.querybuilder(
                from_=BigQuery.querybuilder(union=('distinct', union)),
                select=['ingestion_timestamp', 'tbl'],
                order=['ingestion_timestamp'],
                limit=1000000
            ),
            select=[('MAX(ingestion_timestamp)', 'ingestion_timestamp')]
        )

        rs = self._bq.query(str(builder))

        if rs[0]['ingestion_timestamp'] is not None:
            xcom['last_ingestion_timestamp'] = str(rs[0]["ingestion_timestamp"])
            xcom['has_data'] = True

        context['ti'].xcom_push(key=self._target, value=xcom)


class SetCheckpointOperator(BaseOperator):
    @apply_defaults
    def __init__(self, env, table, *args, **kwargs):
        self._bq = BigQuery(env)
        self._table = table
        super(SetCheckpointOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        checkpoint = context['ti'].xcom_pull(key=self._table)

        if 'last_ingestion_timestamp' in checkpoint:
            # We want to increment the last_ingestion_timestamp by 1 microsecond to prevent
            # checkpoint overlap. See DTS-62 for context. jc - 7/17/2020
            ts = dateutil.parser.parse(checkpoint['last_ingestion_timestamp']).timestamp()
            dt = datetime.fromtimestamp(ts) + timedelta(microseconds=1)
            lit = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            query = f"""
                MERGE `system.checkpoint` AS t
                USING (SELECT '{self._table}' AS table, '{context['ti'].dag_id}' AS dag_id) AS s
                ON t.table = s.table
                AND t.dag_id = s.dag_id
                WHEN MATCHED THEN
                    UPDATE
                        SET t.checkpoint='{lit}'
                WHEN NOT MATCHED THEN
                    INSERT (dag_id, `table`, checkpoint) VALUES
                    ('{context['ti'].dag_id}', "{self._table}", "{lit}")
            """
            self._bq.query(query)

            log.info('successfully set checkpoint for table {}'.format(self._table))


class ScheduleDataflowJobOperator(BaseOperator, SkipMixin):
    @apply_defaults
    def __init__(
        self, project, template_name, job_name, job_parameters={}, pull_parameters={},
        http=None, requestBuilder=None, *args, **kwargs
    ):
        self._project = project
        self._template_name = template_name
        self._job_name = job_name
        self._job_parameters = job_parameters
        self._pull_parameters = pull_parameters
        self._http = http
        self._requestBuilder = requestBuilder
        # Expose so we can unit test. Required by XCOMMs. - Stu M 7/22/20
        self.provide_context = kwargs.get('provide_context', False)

        super(ScheduleDataflowJobOperator, self).__init__(*args, **kwargs)

    def merge_parameters(self, ti):
        pull_params = {}

        for p in self._pull_parameters:
            if 'key' in p:
                if 'param_name' in p:
                    pull_params[p['param_name']] = ti.xcom_pull(key=p['key'])
                else:
                    pull_params[p['key']] = ti.xcom_pull(key=p['key'])
            elif('task_id' in p):
                pull_params[p['param_name']] = ti.xcom_pull(task_ids=p['task_id'])

        pull_params.update(self._job_parameters)
        return pull_params

    def execute(self, context):
        service = GoogleCloudServiceFactory.build('dataflow', http=self._http, requestBuilder=self._requestBuilder)

        job_parameters = context['ti'].xcom_pull(key='job_parameters')
        if isinstance(job_parameters, dict):
            self._job_parameters = {**job_parameters, **self._job_parameters}

        request = service.projects().templates().launch(
            projectId=self._project,
            gcsPath=f'gs://{self._project}-dataflow/templates/{self._template_name}',
            body={
                'jobName': self.safe_job_name(),
                'parameters': self.merge_parameters(context['ti']),
                'environment': {
                    'serviceAccountEmail': f'dataflow@{self._project}.iam.gserviceaccount.com',
                    'machineType': os.environ.get('DATAFLOW_MACHINE_TYPE', 'n1-standard-1')
                }
            }
        )

        try:
            response = request.execute()
        except HttpError as e:
            if e.resp.status == 409:
                downstream_tasks = context['task'].get_flat_relatives(upstream=False)
                if downstream_tasks:
                    log.info('409 status encountered. Skipping downstream tasks because job already \
                             exists with the same name.')
                    self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)
                return
            else:
                raise e

        return response['job']

    def safe_job_name(self):
        job_name = self._job_name
        for ch in ['.']:
            if ch in job_name:
                job_name = job_name.replace(ch, "-")
        return job_name


class OperatorPlugin(AirflowPlugin):
    name = "custom_operators"
    operators = [GetCheckpointOperator, SetCheckpointOperator, ScheduleDataflowJobOperator]
