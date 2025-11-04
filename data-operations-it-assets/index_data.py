import pandas as pd
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

CLOUD_URL = os.getenv("CLOUD_URL")
API_KEY = os.getenv("API_KEY")

# File and index name
EXCEL_FILE = "it_asset_inventory_cleaned.csv"
INDEX_NAME = "it-assets"

es = Elasticsearch(
    CLOUD_URL,
    api_key=API_KEY
)


if es.ping():
    print("Connected to Elasticsearch Cloud.")
else:
    print("Could not connect. Check your CLOUD_URL or API_KEY.")
    exit()


try:
    df = pd.read_csv(EXCEL_FILE)
    print(f"Loaded {len(df)} rows from {EXCEL_FILE}")
except Exception as e:
    print("Error reading file:", e)
    exit()

# Convert DataFrame rows to Elasticsearch documents
actions = [
    {
        "_index": INDEX_NAME,
        "_source": row.to_dict()
    }
    for _, row in df.iterrows()
]

# Bulk upload documents
try:
    success, failed = helpers.bulk(es, actions, stats_only=True)
    print(f"Indexed successfully: {success} documents")
    if failed > 0:
        print(f"{failed} document(s) failed to index.")
except Exception as e:
    print("Error during indexing:", e)

# Verify document count in Elasticsearch
try:
    count = es.count(index=INDEX_NAME)['count']
    print(f"Total documents in index '{INDEX_NAME}': {count}")
except Exception as e:
    print("Could not verify document count:", e)
