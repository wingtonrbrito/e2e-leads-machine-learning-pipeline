from airflow.operators.email_operator import EmailOperator


def report_failure(context):
    subject = 'Airflow critical: {ti}'.format(**context)
    html_content = """
         Exception:<br>{exception}<br>
         Execution Date: {execution_date}<br>
         Log: <a href="{ti.log_url}">Link</a><br>
         Host: {ti.hostname}<br>
         Log file: {ti.log_filepath}<br>
         Mark success: <a href="{ti.mark_success_url}">Link</a><br>
    """.format(**context)
    """Send custom email alerts."""
    return EmailOperator(
        task_id='report_email_failure',
        to='bd480e20-1fac-47b4-9177-edc195918121+dtm@alert.victorops.com',
        subject=subject,
        html_content=html_content,
    ).execute(context)
