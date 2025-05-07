#!/bin/bash
set -e  # Exit immediately if any command fails

# Check if at least one year is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <year1> [year2 ...]"
    exit 1
fi

# Define directories
DATA_DIR="data"
PROCESSED_DIR="processed_data"

# Ensure the processed_data folder exists
mkdir -p "$PROCESSED_DIR"

# Process each provided year
for year in "$@"; do

    YEAR_DIR="$DATA_DIR/$year"
    if [ ! -d "$YEAR_DIR" ]; then
        echo "Warning: Directory for year $year ($YEAR_DIR) does not exist. Skipping..."  >> ./logging/pipeline.log
        continue
    fi

    echo "Processing year: $year" >> ./logging/pipeline.log

    # Iterate over each month folder in the year directory
    for month_path in "$YEAR_DIR"/*; do
        
        if [ -d "$month_path" ]; then
            
            month_folder=$(basename "$month_path")

            echo "  $month_folder: Filters being applied"  >> ./logging/pipeline.log
            python3 filter.py --data-folder "$month_path" --processed-folder "$PROCESSED_DIR"

            echo "  $month_folder: Aggregating processed data"  >> ./logging/pipeline.log
            python3 aggregate.py --processed-folder "$PROCESSED_DIR" --month "$month_folder"
            
            echo "  $month_folder: Ingesting the aggregated data"  >> ./logging/pipeline.log
            python3 ingest.py --processed-folder "$PROCESSED_DIR" --month "$month_folder"

            # Empty the processed_data folder after processing the month
            echo "  $month_folder: Emptying the processed_data folder"  >> ./logging/pipeline.log
            rm -rf "$PROCESSED_DIR"/*

            echo "  $month_folder: Processing complete"  >> ./logging/pipeline.log
            echo "  -" >> ./logging/pipeline.log
        fi
    done

    echo "Processing complete for year: $year."  >> ./logging/pipeline.log
    echo "--" >> ./logging/pipeline.log
done

rmdir $PROCESSED_DIR

echo "All processing complete."  >> ./logging/pipeline.log
