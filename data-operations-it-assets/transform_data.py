import os
from datetime import datetime
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# It copies your cleaned data to a new index (it-assets-transformed)
# Enhances it by adding new fields (risk_level and system_age)
# Cleans it by removing invalid records (missing hostname or unknown provider)


# Load environment variables
load_dotenv()

CLOUD_URL = os.getenv("CLOUD_URL")
API_KEY = os.getenv("API_KEY")

# Elasticsearch connection
es = Elasticsearch(
    CLOUD_URL,
    api_key=API_KEY
)

# Check connection
if es.ping():
    print("Connected to Elasticsearch Cloud!")
else:
    print("Connection failed. Check CLOUD_URL or API_KEY.")
    exit()

# Index names
SOURCE_INDEX = "it-assets"
TARGET_INDEX = "it-assets-transformed"

# 1. Reindex data to another index
print(f"Reindexing from '{SOURCE_INDEX}' to '{TARGET_INDEX}'...")
try:
    es.reindex(
        body={
            "source": {"index": SOURCE_INDEX},
            "dest": {"index": TARGET_INDEX}
        },
        wait_for_completion=True
    )
    print("Reindex complete.")
except Exception as e:
    print("Error during reindex:", e)
    exit()

# 2. Add derived field: risk_level
# If operating_system_lifecycle_status is EOL or EOS → High, else Low
print("Adding risk_level field...")
try:
    es.update_by_query(
        index=TARGET_INDEX,
        body={
            "script": {
                "source": """
                    if (ctx._source.containsKey('operating_system_lifecycle_status')) {
                        def status = ctx._source.operating_system_lifecycle_status.toLowerCase();
                        if (status == 'eol' || status == 'eos') {
                            ctx._source.risk_level = 'High';
                        } else {
                            ctx._source.risk_level = 'Low';
                        }
                    } else {
                        ctx._source.risk_level = 'Low';
                    }
                """,
                "lang": "painless"
            },
            "query": {"match_all": {}}
        },
        wait_for_completion=True
    )
    print("risk_level field added.")
except Exception as e:
    print("Error adding risk_level:", e)

# 3. Calculate system_age (in years) from installation_date
print("Calculating system_age...")
try:
    es.update_by_query(
        index=TARGET_INDEX,
        body={
            "script": {
                "source": """
                    if (ctx._source.containsKey('installation_date') && ctx._source.installation_date != '') {
                        try {
                            def fmt = new java.text.SimpleDateFormat("yyyy-MM-dd");
                            def installDate = fmt.parse(ctx._source.installation_date);
                            def now = new java.util.Date();
                            def diff = now.getTime() - installDate.getTime();
                            def years = Math.floor(diff / (1000 * 60 * 60 * 24 * 365));
                            ctx._source.system_age = (int) years;
                        } catch (Exception e) {
                            ctx._source.system_age = null;
                        }
                    }
                """,
                "lang": "painless"
            },
            "query": {"match_all": {}}
        },
        wait_for_completion=True
    )
    print("system_age calculated.")
except Exception as e:
    print("Error calculating system_age:", e)

# 4. Delete records with missing hostname or Unknown providers
print("Deleting invalid records (missing hostname or Unknown provider)...")
try:
    es.delete_by_query(
        index=TARGET_INDEX,
        body={
            "query": {
                "bool": {
                    "should": [
                        {"bool": {"must_not": {"exists": {"field": "hostname"}}}},
                        {"term": {"provider.keyword": "Unknown"}}
                    ],
                    "minimum_should_match": 1
                }
            }
        },
        wait_for_completion=True
    )
    print("Invalid records deleted.")
except Exception as e:
    print("Error deleting records:", e)

# 5. Verify total documents after transformation
try:
    count = es.count(index=TARGET_INDEX)['count']
    print(f"Total documents after transformation: {count}")
except Exception as e:
    print("Could not count documents:", e)
