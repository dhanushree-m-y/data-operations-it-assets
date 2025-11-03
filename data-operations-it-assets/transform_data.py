import os
import math
from datetime import datetime
from dateutil import parser as date_parser
from elasticsearch import Elasticsearch, helpers, exceptions


CLOUD_URL = os.getenv(
    "CLOUD_URL",
    "https://it-asset-data-operation-e19ded.es.us-central1.gcp.elastic.cloud:443"
)
API_KEY = os.getenv(
    "ES_API_KEY",
    "cmFvNlNKb0JYbWVzTktSYVg0a3M6Q3dpT0pJXzVwOG55R0F6b1U3bGtjUQ=="
)

SOURCE_INDEX = "it-assets"
TARGET_INDEX = "it-assets-transformed"
BATCH_SIZE = 500

es = Elasticsearch(CLOUD_URL, api_key=API_KEY)


def ensure_connection():
    """Ensure connection to Elasticsearch."""
    try:
        if es.ping():
            print("Connected to Elasticsearch Cloud.")
        else:
            raise RuntimeError("Elasticsearch ping failed.")
    except Exception as e:
        raise RuntimeError(f"Connection error: {e}")


def reindex_to_target(source=SOURCE_INDEX, target=TARGET_INDEX):
    """Reindex data from source to target index."""
    if not es.indices.exists(index=source):
        raise RuntimeError(f"Source index '{source}' does not exist.")

    if es.indices.exists(index=target):
        print(f"Target index '{target}' already exists — deleting it.")
        es.indices.delete(index=target)

    print(f"Reindexing from '{source}' to '{target}' ...")
    body = {"source": {"index": source}, "dest": {"index": target}}

    resp = es.reindex(body=body, wait_for_completion=True, request_timeout=3600)
    print("Reindex complete:", resp.get("created", "OK"), "documents copied.")


def delete_bad_records(target=TARGET_INDEX):
    """Delete documents missing hostname or with Unknown provider."""
    query = {
        "bool": {
            "should": [
                {"bool": {"must_not": [{"exists": {"field": "hostname"}}]}},
                {"term": {"hostname.keyword": ""}},
                {"terms": {
                    "provider.keyword": [
                        "Unknown", "unknown", "UNKNOWN",
                        "N/A", "Not Available", "NA"
                    ]
                }}
            ]
        }
    }

    print("Deleting invalid records (missing hostname or Unknown provider)...")
    resp = es.delete_by_query(
        index=target,
        body={"query": query},
        refresh=True,
        wait_for_completion=True,
        request_timeout=3600
    )
    print(f"Deleted {resp.get('deleted', 0)} invalid records.")


def parse_date_try(value):
    """Try to parse various date formats safely."""
    if not value:
        return None

    try:
        return date_parser.parse(str(value))
    except Exception:
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(str(value), fmt)
            except Exception:
                continue
    return None


def compute_system_age_years(install_date):
    """Compute system age in years."""
    if not install_date:
        return None

    now = datetime.utcnow()
    delta_days = (now - install_date).days
    return max(0, int(delta_days // 365.25))


def derive_risk(status):
    """Determine risk level based on OS lifecycle status."""
    if not status:
        return "Low"

    status = str(status).strip().upper()
    if status in ("EOL", "EOS", "END OF LIFE", "END-OF-SUPPORT"):
        return "High"
    return "Low"


def bulk_update_documents(target=TARGET_INDEX):
    """Add derived fields to each document."""
    print("Updating documents with new fields (risk_level, system_age)...")

    scan_iter = helpers.scan(
        es,
        index=target,
        query={"query": {"match_all": {}}},
        size=1000,
        request_timeout=600
    )

    actions = []
    total = updated = 0

    for doc in scan_iter:
        total += 1
        _id = doc["_id"]
        src = doc.get("_source", {})

        os_status = src.get("operating_system_lifecycle_status")
        risk_level = derive_risk(os_status)

        install_date = src.get("installation_date")
        parsed_date = parse_date_try(install_date)
        system_age = compute_system_age_years(parsed_date)

        update_doc = {"risk_level": risk_level, "system_age": system_age}

        actions.append({
            "_op_type": "update",
            "_index": target,
            "_id": _id,
            "doc": update_doc
        })

        if len(actions) >= BATCH_SIZE:
            helpers.bulk(es, actions, refresh=True)
            updated += len(actions)
            print(f"Updated {updated} documents...")
            actions = []

    if actions:
        helpers.bulk(es, actions, refresh=True)
        updated += len(actions)

    print(f"Total documents processed: {total}")
    print(f"Successfully updated: {updated}")


def main():
    """Run the full transformation workflow."""
    ensure_connection()
    reindex_to_target()
    delete_bad_records()
    bulk_update_documents()
    print("Data transformation completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except exceptions.ConnectionError as e:
        print("Connection error:", e)
    except Exception as e:
        print("Error:", e)
