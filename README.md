# FCC MBA Ingestion & Processing Pipeline


This repository automates downloading, cleaning, aggregating, and ingesting FCC Measuring Broadband America (MBA) raw data into Elasticsearch, plus an operator-identification step.

## Project Structure

```
.
├── data/                        # Download & conversion helpers
├── elastic/                     # Elasticsearch security assets
├── logging/                     # Logs for each stage
├── filter.py                    # Filter, timezone-adjust, classify on/off-net
├── aggregate.py                 # Compute throughput, latency, jitter aggregates
├── ingest.py                    # Bulk-load aggregated Parquet into Elasticsearch
├── operator_identification.py   # Map each unit to its ISP and technology
├── run.sh                       # One/Multi-year pipeline: filter → aggregate → ingest
├── identify_operators.sh        # One/Multi-year operator-ID pass
├── run_all.sh                   # Loop over all years, choose mode via `--op-id`
├── constants.py                 # File names, filter rules, hostname mappings
└── requirements.txt             # Python dependencies

```

## Prerequisites

- **Python 3.8+**  
- **Bash** shell  
- Install dependencies:
```bash
  pip install -r requirements.txt
```

## End-to-End Pipeline

1. **Download & convert**
   Place raw CSVs under `data/<YYYYMM>` (see `data/README.md`)

2. **Operator Identification**
   Independently classify each unit_id by ISP and technology:

   ```bash
   bash identify_operators.sh 2023
   ```

   Generates `data/2023MM/unit_id_mapping.csv` for all months `MM` for year `2023`.

3. **Run transform & ingest**
   For a single year:

   ```bash
   bash run.sh 2023
   ```

   * **filter.py** applies QA filters, timezone localization, and on/off-net classification.
   * **aggregate.py** groups by unit_id/month and computes summary metrics.
   * **ingest.py** pushes Parquet to Elasticsearch indices `aggregated-<metric>-<YYYYMM>`.


4. **Process all years**
   Master orchestrator:

   ```bash
   # For operator identification mode
   bash run_all.sh --op-id
   # For processing and ingesting data
   bash run_all.sh
   
   ```

## Scripts Overview

* **filter.py**
  Applies file-specific filters (`constants.FILTER_MAP`), fills missing timestamps via unit timezones, and labels peak/off-peak or weekend.

* **aggregate.py**
  Aggregates throughput, latency, or jitter data into monthly summaries; outputs Parquet to `processed_data/`.

* **ingest.py**
  Reads `processed_data/*.parquet` and bulk-indexes into Elasticsearch (config in `elastic/security.ini`).

* **operator\_identification.py**
  Merges target-hostname classification with unit profiles to build a comprehensive unit→ISP mapping.

* **run.sh / identify\_operators.sh**
  Invoke the above in sequence for specified years.

* **run\_all.sh**
  Batch-processes all years (2011–2023), with optional `--op-id` flag for operator identification only.

## Logging

All stages write logs under `logging/`:

* `filter.log`
* `aggregate.log`
* `ingest.log`
* `operator_identification.log`
* `pipeline.log`

Monitor these for errors or processing summaries.
