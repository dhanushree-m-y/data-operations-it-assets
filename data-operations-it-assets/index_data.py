import pandas as pd
from elasticsearch import Elasticsearch, helpers


CLOUD_URL = "https://it-asset-data-operation-e19ded.es.us-central1.gcp.elastic.cloud:443"  # Replace with your Cloud endpoint
API_KEY = "cmFvNlNKb0JYbWVzTktSYVg0a3M6Q3dpT0pJXzVwOG55R0F6b1U3bGtjUQ=="  # Replace with your actual API key
EXCEL_FILE = "it_asset_inventory_cleaned.csv"
INDEX_NAME = "it-assets"


es = Elasticsearch(
    CLOUD_URL,
    api_key=API_KEY
)

# Test connection
if es.ping():
    print("Connected to Elasticsearch Cloud!")
else:
    print("Could not connect. Check your CLOUD_URL or API_KEY.")
    exit()

# Load Excel data
try:
    df = pd.read_csv(EXCEL_FILE)
    print(f"Loaded {len(df)} rows from {EXCEL_FILE}")
except Exception as e:
    print("Error reading Excel file:", e)
    exit()

# Prepare data for bulk indexing
actions = [
    {
        "_index": INDEX_NAME,
        "_source": row.dropna().to_dict()
    }
    for _, row in df.iterrows()
]


try:
    success, failed = helpers.bulk(es, actions, stats_only=True)
    print(f"Indexed successfully: {success} documents")
    if failed > 0:
        print(f"{failed} document(s) failed to index.")
except Exception as e:
    print("Error during indexing:", e)
