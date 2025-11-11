from datetime import datetime
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch("http://elasticsearch:9200", verify_certs=False)
index_name = "job_logs"

# Ensure index exists with mapping
if not es.indices.exists(index=index_name):
    mapping = {
        "mappings": {
            "properties": {
                "sourcename": {"type": "keyword"},
                "tablename": {"type": "keyword"},
                "stage": {"type": "keyword"},
                "status": {"type": "keyword"},
                "message": {"type": "text"},
                "time": {"type": "date"}
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)
    print(f"✅ Index '{index_name}' created.")
else:
    print(f"ℹ️ Index '{index_name}' already exists.")

def log_job_status(sourcename, tablename, stage, status, message=None):
    """
    Logs job status into Elasticsearch for tracking ETL steps.
    """
    log_entry = {
        "sourcename": sourcename,
        "tablename": tablename,
        "stage": stage,
        "status": status,
        "message": message,
        "time": datetime.utcnow().isoformat()
    }
    es.index(index=index_name, document=log_entry)
    print(f"🧾 LOGGED: [{tablename}] {stage} - {status}")
