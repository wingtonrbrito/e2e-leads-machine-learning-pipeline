from google.cloud import bigquery
from google.cloud import aiplatform
from typing import List, Dict
import logging
from .utils import get_bq_client, handle_bq_exceptions

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    def __init__(self, project_id: str, dataset_id: str, table_id: str, endpoint_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.endpoint_id = endpoint_id
        self.bq_client = get_bq_client(project_id)
        aiplatform.init(project=project_id, location='us-central1')
        self.endpoint = aiplatform.Endpoint(self.endpoint_id)

    @handle_bq_exceptions
    def fetch_text_data(self) -> List[Dict]:
        """Fetch text data for sentiment analysis."""
        query = f"""
            SELECT lead_id, feedback_text 
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE sentiment_processed = False
        """
        return self.bq_client.query(query).to_dataframe()

    def analyze_sentiment(self, texts: List[str]) -> List[Dict]:
        """Get sentiment predictions from Vertex AI."""
        instances = [{"text": text} for text in texts]
        response = self.endpoint.predict(instances=instances)
        return [
            {"label": pred["label"], "score": pred["score"]}
            for pred in response.predictions
        ]

    @handle_bq_exceptions
    def update_sentiment(self, data, results: List[Dict]) -> None:
        """Update BigQuery with sentiment results."""
        data[['sentiment_label', 'sentiment_score']] = results
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        job = self.bq_client.load_table_from_dataframe(data, table_ref)
        job.result()
        logger.info("Sentiment analysis results written")

    def execute(self):
        """Orchestrate sentiment analysis pipeline."""
        data = self.fetch_text_data()
        if data.empty:
            logger.warning("No text data