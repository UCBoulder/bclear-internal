## 1. Data Download & Storage

1. **Download & extract** raw tar files for each year/month via  
   `get_data.sh` → `data/<YYYY><MM>`
2. **Remove leading “#”** from CSV header lines using  
   `remove_hash.sh`
3. **Insert column headers** per the schema in README.md (with `sed -i 1i…` commands) 
4. **Convert to Parquet** for efficient storage via  
   `convert.py` 

## 2. Operator Identification

1. **Loop over years/months** with  
   `identify_operators.sh` → calls `operator_identification.py` 
2. **In `operator_identification.py`:**  
   - Load all raw Parquet files (`FILE_LIST`) and collect unique unit IDs 
   - Group by target hostname to assign a preliminary operator (`group_by_targets`) 
   - Filter out “off-net” or `no_operator`, clean names into `operator_name` 
   - Load year-specific unit profiles from `UNIT_PROFILE_FILE` to classify remaining units 
   - Apply `OPERATOR_TECHNOLOGY_MAPPING` and speed-based rules from `OPERATOR_TECHNOLOGY_YEAR` to refine technology
   - Write final **unit_id → operator_name/operator_technology** mapping to a CSV/Parquet file  

## 3. Run Pipeline (Transform & Ingest)

1. **Invoke** `run.sh` (or via the `run_all.sh` wrapper) to loop each year → each month 
2. For **each month** folder:  
   - **Filter** raw Parquets with `filter.py` (using `FILTER_MAP`, timezone & on-net/off-net rules) → write cleaned Parquets to `processed_data`  
   - **Aggregate** filtered data with `aggregate.py` (latency, throughput, packet-loss bins, etc.) → write aggregated Parquets 
   - **Ingest** aggregates into Elasticsearch with `ingest.py` (bulk-index per measurement)
   - **Clean up** `processed_data` before continuing  

