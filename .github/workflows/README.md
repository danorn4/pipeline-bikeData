# Capital Bikeshare Live & Historical Data Pipeline

[![Ingest Capital Bikeshare Live Feed](https://github.com/danorn4/pipeline-bikeData/actions/workflows/ingest.yml/badge.svg)](https://github.com/danorn4/pipeline-bikeData/actions/workflows/ingest.yml)

A **$0-cost, end-to-end cloud data pipeline** that combines 12 months of historical Capital Bikeshare trip data with automated, near-real-time station availability feeds in **Google BigQuery Sandbox**, powering live analytics in **Looker Studio**.

---

## Architecture

```text
Historical Backfill (One-Time)
  Capital Bikeshare Monthly CSVs (.zip)
        │
        ▼ (seed_trips.py — streaming via BytesIO)
  BigQuery: bikeshare.trips (6.24M rows | 0.83 GB)

Live Ingestion (Automated Every 15 Minutes)
  Capital Bikeshare GBFS API (station_information.json & station_status.json)
        │
        ▼ (GitHub Actions + ingest_live.py)
  BigQuery: bikeshare.station_information & bikeshare.station_status_raw
        │
        ▼ (SQL Views: deduplication + live-to-history join)
  Looker Studio Dashboard (Liveness & Station Pressure Map)