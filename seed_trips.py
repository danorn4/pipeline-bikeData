import glob
import io
import os
import zipfile
from google.cloud import bigquery

# 1. Config — automatically uses the project from your service account JSON
client = bigquery.Client()
TABLE_REF = f"{client.project}.bikeshare.trips"
DATA_DIR = "historical_data"

# 2. Explicit schema (prevents autodetect from guessing INT64 for station IDs)
SCHEMA = [
    bigquery.SchemaField("ride_id", "STRING"),
    bigquery.SchemaField("rideable_type", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("ended_at", "TIMESTAMP"),
    bigquery.SchemaField("start_station_name", "STRING"),
    bigquery.SchemaField("start_station_id", "STRING"),
    bigquery.SchemaField("end_station_name", "STRING"),
    bigquery.SchemaField("end_station_id", "STRING"),
    bigquery.SchemaField("start_lat", "FLOAT64"),
    bigquery.SchemaField("start_lng", "FLOAT64"),
    bigquery.SchemaField("end_lat", "FLOAT64"),
    bigquery.SchemaField("end_lng", "FLOAT64"),
    bigquery.SchemaField("member_casual", "STRING"),
]


def load_historical_zips():
    zip_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.zip")))
    if not zip_files:
        raise FileNotFoundError(f"No .zip files found in '{DATA_DIR}/'")

    print(f"Found {len(zip_files)} zip files. Loading into {TABLE_REF}...")

    for i, zip_path in enumerate(zip_files):
        # First file wipes any partial prior run; remaining files append
        write_mode = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if i == 0
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=write_mode,
        )

        with zipfile.ZipFile(zip_path, "r") as zf:
            # Filter out hidden __MACOSX metadata files that Capital Bikeshare zips often include
            csv_names = [
                name
                for name in zf.namelist()
                if name.endswith(".csv") and not name.startswith("__MACOSX")
            ]
            if not csv_names:
                print(f"Skipping {zip_path}: no CSV found inside.")
                continue

            csv_name = csv_names[0]
            print(f"[{i + 1}/{len(zip_files)}] Loading {csv_name} ({write_mode})...")

            csv_bytes = io.BytesIO(zf.read(csv_name))
            job = client.load_table_from_file(
                csv_bytes, TABLE_REF, job_config=job_config
            )
            job.result()  # Wait for BigQuery to finish this file

    # Print final stats for your capacity-planning writeup
    table = client.get_table(TABLE_REF)
    size_gb = table.num_bytes / (1024**3)
    print(f"\nDone! Loaded {table.num_rows:,} total rows ({size_gb:.2f} GB).")


if __name__ == "__main__":
    load_historical_zips()