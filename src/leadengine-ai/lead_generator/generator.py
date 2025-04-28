from google.cloud import bigquery
from google.cloud import aiplatform
from typing import List, Dict
import logging
from .utils import get_bq_client, handle_bq_exceptions

logger = logging.getLogger(__name__)

class LeadGenerator:
    def __init__(self, project_id: str, dataset_id: str, table_id: str, endpoint_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.endpoint_id = endpoint_id
        self.bq_client = get_bq_client(project_id)
        aiplatform.init(project=project_id, location='us-central1')
        self.endpoint = aiplatform.Endpoint(self.endpoint_id)

    @handle_bq_exceptions
    def fetch_data(self) -> List[Dict]:
        """Fetch processed lead data from BigQuery."""
        query = f"""
            SELECT * 
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE processed = True
        """
        return self.bq_client.query(query).to_dataframe()

    def predict_scores(self, data) -> List[float]:
        """Get lead scores from Vertex AI endpoint."""
        instances = data[['age', 'engagement_score', 'page_views']].to_dict('records')
        response = self.endpoint.predict(instances=instances)
        return [prediction[0] for prediction in response.predictions]

    @handle_bq_exceptions
    def write_results(self, data, scores: List[float]) -> None:
        """Write lead scores back to BigQuery."""
        data['lead_score'] = scores
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        job = self.bq_client.load_table_from_dataframe(data, table_ref)
        job.result()
        logger.info("Lead scores updated in BigQuery")

    def execute(self):
        """Orchestrate lead scoring pipeline."""
        data = self.fetch_data()
        if data.empty:
            logger.warning("No processed data found")
            return
        scores = self.predict_scores(data)
        self.write_results(data, scores)