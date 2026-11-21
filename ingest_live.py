import json
import urllib.request
from datetime import datetime, timezone
from google.cloud import bigquery

client = bigquery.Client()
DATASET = f"{client.project}.bikeshare"

STATION_INFO_URL = "https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json"
STATION_STATUS_URL = "https://gbfs.capitalbikeshare.com/gbfs/en/station_status.json"

INFO_SCHEMA = [
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("short_name", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("lat", "FLOAT64"),
    bigquery.SchemaField("lon", "FLOAT64"),
    bigquery.SchemaField("capacity", "INT64"),
    bigquery.SchemaField("region_id", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

STATUS_SCHEMA = [
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("num_bikes_available", "INT64"),
    bigquery.SchemaField("num_ebikes_available", "INT64"),
    bigquery.SchemaField("num_bikes_disabled", "INT64"),
    bigquery.SchemaField("num_docks_available", "INT64"),
    bigquery.SchemaField("num_docks_disabled", "INT64"),
    bigquery.SchemaField("is_installed", "INT64"),
    bigquery.SchemaField("is_renting", "INT64"),
    bigquery.SchemaField("is_returning", "INT64"),
    bigquery.SchemaField("last_reported", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def unix_to_iso(ts) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def ingest_station_info(now_iso: str):
    payload = fetch_json(STATION_INFO_URL)
    stations = payload["data"]["stations"]

    rows = [
        {
            "station_id": str(s["station_id"]),
            "short_name": str(s.get("short_name")) if s.get("short_name") is not None else None,
            "name": s.get("name"),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "capacity": s.get("capacity"),
            "region_id": str(s.get("region_id")) if s.get("region_id") is not None else None,
            "ingested_at": now_iso,
        }
        for s in stations
    ]

    job_config = bigquery.LoadJobConfig(
        schema=INFO_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, f"{DATASET}.station_information", job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} rows into station_information (WRITE_TRUNCATE).")


def ingest_station_status(now_iso: str):
    payload = fetch_json(STATION_STATUS_URL)
    stations = payload["data"]["stations"]

    rows = [
        {
            "station_id": str(s["station_id"]),
            "num_bikes_available": s.get("num_bikes_available"),
            "num_ebikes_available": s.get("num_ebikes_available"),
            "num_bikes_disabled": s.get("num_bikes_disabled"),
            "num_docks_available": s.get("num_docks_available"),
            "num_docks_disabled": s.get("num_docks_disabled"),
            "is_installed": s.get("is_installed"),
            "is_renting": s.get("is_renting"),
            "is_returning": s.get("is_returning"),
            "last_reported": unix_to_iso(s.get("last_reported")),
            "ingested_at": now_iso,
        }
        for s in stations
    ]

    job_config = bigquery.LoadJobConfig(
        schema=STATUS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, f"{DATASET}.station_status_raw", job_config=job_config)
    job.result()
    print(f"Appended {len(rows)} rows into station_status_raw (WRITE_APPEND).")


if __name__ == "__main__":
    now_iso = datetime.now(timezone.utc).isoformat()
    ingest_station_info(now_iso)
    ingest_station_status(now_iso)