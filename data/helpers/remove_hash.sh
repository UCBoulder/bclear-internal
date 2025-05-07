#!/bin/bash
# Usage: ./remove_header_hash.sh [directory]
# If no directory is provided, the current directory is used.

# Set the target directory (default to current directory)
DIRECTORY="${1:-.}"

# Loop through each CSV file in the directory
for file in "$DIRECTORY"/*.csv; do
    if [[ -f "$file" ]]; then
        # Remove a leading '#' from the first line only
        sed -i '1s/^#//' "$file"
        echo "Processed $file"
    fi
done
