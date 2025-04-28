# e2e-leads-machine-learning-pipeline

[![Build Status](https://img.shields.io/github/actions/workflow/status/your-org/e2e-leads-machine-learning-pipeline/ci.yml?branch=main&style=flat-square)](https://github.com/your-org/e2e-leads-machine-learning-pipeline/actions)  
[![License](https://img.shields.io/github/license/your-org/e2e-leads-machine-learning-pipeline?style=flat-square)](LICENSE)

**Production-grade** framework for end-to-end lead enrichment on GCP, featuring:

- ✅ **Tech Stack:** Python · Apache Airflow · Cloud Dataflow · BigQuery · Vertex AI  
- ✅ **Infrastructure:** Google Cloud (Storage, Dataflow, BigQuery, Vertex AI) · Docker  
- ✅ **CI/CD:** GitHub Actions for linting, testing, and deployment  
- ✅ **Patterns:** Batch & real-time ML scoring · DAG orchestration  
- ✅ **Monitoring & Metrics:** Vertex AI Model Monitoring · Stackdriver Logging  

---

## Description

This repo demonstrates a scalable ML pipeline that:

- **Ingests** raw visitor data into GCS  
- **Processes** and transforms it via Cloud Dataflow  
- **Stores** structured tables in BigQuery  
- **Enriches** with Vertex AI for sentiment analysis & lead scoring  
- **Orchestrates** via Airflow for both batch and live workloads  
- **Delivers** actionable insights to sales & marketing teams  

---

## 🚀 Quick Start

1. **Clone & Install**  
   ```bash
   git clone git@github.com:your-org/e2e-leads-machine-learning-pipeline.git
   cd e2e-leads-machine-learning-pipeline
   pip install -r requirements.txt

	2.	Configure
Create a .env file or export:

GCP_PROJECT_ID=<your-project-id>
GCP_REGION=<your-region>
BQ_DATASET=<your_dataset>
LEAD_MODEL_ENDPOINT=<vertex-ai-endpoint-id>
SENTIMENT_MODEL_ENDPOINT=<vertex-ai-endpoint-id>


	3.	Trigger Pipelines

airflow dags trigger load_gcs_to_lake
airflow dags trigger leads_records



⸻

Architecture Diagram

Cloud Storage (client/user visitors)
   ↓
load_gcs_to_lake (DAG)
   ↓
BigQuery Lake + Dataflow (ETL processing, cleaning)
   ↓
BigQuery (Processed Data)
   ↓
Vertex AI:
    ├── Sentiment Analysis (e.g., on feedback, messages)
    └── Lead Scoring Model (conversion prediction)
   ↓
LeadsAI (internal ML orchestration using Vertex AI predictions)
   ↓
leads_records (DAG)
   ↓
BigQuery Leads_Records (Scored + Sentiment-tagged Leads)



⸻

⚡ Flow Explained in Steps
	1.	Ingest raw data into GCS
	2.	Load into BigQuery Lake via load_gcs_to_lake DAG
	3.	Clean & Transform with Cloud Dataflow
	4.	Store processed datasets in BigQuery
	5.	Enrich using Vertex AI:
	•	Sentiment Analysis on text
	•	Lead Quality Scoring on structured features
	•	Persist predictions back to BigQuery
	6.	Orchestrate downstream tasks in leads_records DAG
	7.	Serve enriched leads from BigQuery_Leads_Records

⸻

🔍 Different Roles: Sentiment vs Lead Quality Score
	•	Sentiment Analysis
	•	Inputs: Unstructured text (reviews, chat transcripts, emails)
	•	Outputs: Polarity (positive|neutral|negative) + strength score
	•	Value: Measure lead enthusiasm and concerns
	•	Lead Quality Score
	•	Inputs: Structured features (demographics, behavior, CRM data)
	•	Outputs: Conversion probability (0–1) or categorical label (hot/warm/cold)
	•	Value: Prioritize leads for sales outreach

Combining both signals yields a holistic lead profile for smarter routing and higher ROI.

⸻

🚀 Practical Usage
	1.	Batch Scoring
Schedule Vertex AI Batch Prediction for bulk updates.
	2.	Real-time Scoring
Deploy Vertex AI Endpoints for live lead scoring in your apps.
	3.	Result Integration
Use Airflow SQL operators to merge predictions into core tables.
	4.	Rule-based Prioritization
Define business rules (e.g., high-score/negative sentiment → human review).
	5.	Monitoring & Retraining
Enable Model Monitoring and automate retraining on drift detection.

⸻

📊 Combined Example in BigQuery Schema

lead_id	name	lead_score	sentiment_score	sentiment_label	final_priority
123	John Doe	0.83	0.75	Positive	High Priority Lead
456	Jane Smith	0.90	-0.60	Negative	Review Manually Before Sales
789	Bob White	0.35	0.80	Positive	Consider for Nurturing



⸻

⚙️ Tech & Dependency References
	•	Runtime Libraries:
	•	boto3==1.7.84
	•	google-cloud-secret-manager==2.0.0
	•	neo4j==4.0.0
	•	pycloudsqlproxy==0.0.15
	•	pyconfighelper==0.0.9
	•	pymysql==0.9.3
	•	typing-extensions==3.7.4.3
	•	virtualenv==20.0.31

⸻

📦 Installation & Setup

Dependencies and packaging are defined in setup.py. To install:

pip install .

# setup.py excerpt
from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'google-cloud-bigquery==1.27.2',
    'google-api-python-client==1.10.0',
    'google-cloud-storage==1.30.0',
    'google-cloud-secret-manager==2.0.0',
    'neo4j==4.0.0',
    'gunicorn==20.0.4',
    'flask==1.1.2',
    'flask-jwt-extended==3.24.1',
    'flask-restful==0.3.8',
    'flask-mail==0.9.1',
    'pyjwt==1.7.1',
    'flask_restful_swagger_3==0.1',
    'swagger-ui-py==0.3.0',
    'locust==1.3.0'
]

setup(
    name='e2e_leads_ml_pipeline',
    version='0.1.0',
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
)

