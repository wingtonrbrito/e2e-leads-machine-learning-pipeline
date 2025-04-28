from google.cloud import bigquery
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def get_bq_client(project_id: str) -> bigquery.Client:
    """Initialize and return BigQuery client."""
    return bigquery.Client(project=project_id)

def handle_bq_exceptions(func):
    """Decorator to handle BigQuery exceptions."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"BigQuery operation failed: {str(e)}")
            raise
    return wrapper