#!/bin/bash

# Main script to process all years using the run script.
# If --operator_identification is given, it uses run_operator.sh instead.

start=$(date +%s)

# Create logging directory if it doesn't exist
if [ ! -d logging ]; then
    mkdir logging
fi

date > ./logging/pipeline.log

# Check for --operator_identification flag among the arguments.
OPERATOR_IDENTIFICATION=false

for arg in "$@"; do
    if [ "$arg" == "--op-id" ]; then
        OPERATOR_IDENTIFICATION=true
        break
    fi
done

if $OPERATOR_IDENTIFICATION; then
    
    RUN_YEAR_SCRIPT="./identify_operators.sh"
    echo "Operator identification mode." >> ./logging/pipeline.log

    if [ -e ./logging/operator_identification.log ]; then
        rm ./logging/operator_identification.log
    fi

    if [ -e ./logging/operator_identification_run.log ]; then
        rm ./logging/operator_identification_run.log
    fi

else
    RUN_YEAR_SCRIPT="./run.sh"
    echo "Transform and Ingest mode." >> ./logging/pipeline.log

    # Remove old filter log if it exists
    if [ -e ./logging/filter.log ]; then
        rm ./logging/filter.log
    fi

    # Remove old aggregation log if it exists
    if [ -e ./logging/aggregate.log ]; then
        rm ./logging/aggregate.log
    fi

    # Remove old ingestion log if it exists
    if [ -e ./logging/ingest.log ]; then
        rm ./logging/ingest.log
    fi

fi

# Ensure the selected run script exists and is executable
if [ ! -x "$RUN_YEAR_SCRIPT" ]; then
    echo "Error: $RUN_YEAR_SCRIPT does not exist or is not executable." >> ./logging/pipeline.log
    exit 1
fi


# Hardcoded list of years to process
YEARS=(2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023)

# Loop through each year and call the selected run script
for YEAR in "${YEARS[@]}"; do
    echo "Starting for year: $YEAR" >> ./logging/pipeline.log
    # Run the selected script for the given year.
    # If it fails, log an error and continue to the next year.
    "$RUN_YEAR_SCRIPT" "$YEAR" || echo "Error processing year $YEAR. Skipping to next year." >> ./logging/pipeline.log
done

echo "All years processed." >> ./logging/pipeline.log

end=$(date +%s)
runtime=$((end - start))
echo "The script took a total time of ${runtime} seconds." >> ./logging/pipeline.log
