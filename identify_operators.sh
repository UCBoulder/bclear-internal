#!/bin/bash
set -e  # Exit immediately if any command fails

# Check if at least one year is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <year1> [year2 ...]"
    exit 1
fi

# Define directories
DATA_DIR="data"


# Process each provided year
for year in "$@"; do

    YEAR_DIR="$DATA_DIR/$year"
    if [ ! -d "$YEAR_DIR" ]; then
        echo "Warning: Directory for year $year ($YEAR_DIR) does not exist. Skipping..."  >> ./logging/operator_identification.log
        continue
    fi

    echo "Processing year: $year" >> ./logging/operator_identification.log

    # Iterate over each month folder in the year directory
    for month_path in "$YEAR_DIR"/*; do
        
        if [ -d "$month_path" ]; then
            
            month_folder=$(basename "$month_path")

            echo "  $month_folder: Operator Identification in progress"  >> ./logging/operator_identification.log
            python3 operator_identification.py --data-folder "$month_path"

            echo "  $month_folder: Operator Identification complete"  >> ./logging/operator_identification.log
            echo "  -" >> ./logging/operator_identification.log
        fi
    done

    echo "Processing complete for year: $year."  >> ./logging/operator_identification.log
    echo "--" >> ./logging/operator_identification.log
done

echo "All processing complete."  >> ./logging/operator_identification.log
