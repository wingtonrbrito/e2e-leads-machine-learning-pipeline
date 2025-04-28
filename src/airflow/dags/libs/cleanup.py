from airflow.utils.db import provide_session
from airflow.models.xcom import XCom


@provide_session
def cleanup_xcom(context, session=None):
    session.query(XCom) \
           .filter(XCom.dag_id == context['ti'].dag_id) \
           .filter(XCom.execution_date == context['ti'].execution_date) \
           .delete()
