#!/usr/bin/env bash

# Bash script to download, and extract FCC MBA data files
# required for analyses


##############################################################################
#  Strict mode and trap for errors
##############################################################################
set -euo pipefail
trap 'echo "Error: Script encountered an error. Exiting." >&2; exit 1' ERR


##############################################################################
#  URL dictionaries, placeholders only. Fill in your actual links.
##############################################################################
declare -A URLS_2011=(
    [2]="https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-feb-2011.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-mar-2011.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-apr-2011.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-may-2011.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-jun-2011.tar.gz"
    [7]="https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201107.tar.gz"
    [8]="https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201108.tar.gz"
    [9]="https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201109.tar.gz"
    [10]="https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201110.tar.gz"
    [11]="https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201111.tar.gz"
    [12]="https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201112.tar.gz"
)

declare -A URLS_2012=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-june.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-july.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-dec.tar.gz"
)

declare -A URLS_2013=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-june.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-july.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-dec.tar.gz"
)

declare -A URLS_2014=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2015/data-raw-2014-dec.tar.gz"
)

declare -A URLS_2015=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-dec.tar.gz"
)

declare -A URLS_2016=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-dec.tar.gz"
)

declare -A URLS_2017=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-dec.tar.gz"
)

declare -A URLS_2018=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-dec.tar.gz"
)

declare -A URLS_2019=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-sept.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-dec.tar.gz"
)

declare -A URLS_2020=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-aug.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-dec.tar.gz"
)

declare -A URLS_2021=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-aug.tar.gz"
    [9]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-sep.tar.gz"
    [10]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-oct.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-dec.tar.gz"
)

declare -A URLS_2022=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-feb.tar.gz"
    [3]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-june.tar.gz" 
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-jul.tar.gz"
    [8]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-aug.tar.gz"
    [11]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-nov.tar.gz"
    [12]="https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-dec.tar.gz"
)

declare -A URLS_2023=(
    [1]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-jan.tar.gz"
    [2]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-feb.tar.gz"
    # March 2023's data is corrupted - Last checked (Jan 2025) 
    # [3]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-mar.tar.gz"
    [4]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-apr.tar.gz"
    [5]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-may.tar.gz"
    [6]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-jun.tar.gz"
    [7]="https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-jul.tar.gz"
)


##############################################################################
# Helper function: download_file
##############################################################################
download_file() {
    local url="$1"
    local dest_path="$2"

    echo "Downloading $url ..."
    # Use curl or wget; -f ensures curl fails on HTTP errors like 404/5xx
    curl -fL -o "$dest_path" "$url"
    echo "Saved to $dest_path"
}


##############################################################################
# Helper function: extract_tar_gz
##############################################################################
extract_tar_gz() {
    local tar_path="$1"
    local extract_to="$2"

    echo "Extracting $tar_path to $extract_to ..."

    # Safety checks
    if [[ ! -f "$tar_path" ]]; then
        echo "Error: '$tar_path' does not exist. Cannot extract."
        return 1
    fi
    if ! tar -tzf "$tar_path" &>/dev/null; then
        echo "Error: '$tar_path' is not a valid tar.gz or is corrupted."
        return 1
    fi

    # Avoid root folder for extraction
    if [[ -z "$extract_to" || "$extract_to" == "/" ]]; then
        echo "Error: extract_to is '$extract_to'; unsafe. Aborting."
        return 1
    fi

    mkdir -p "$extract_to"
    tar -xf "$tar_path" -C "$extract_to"
    echo "Extraction complete."

    # Flatten the directory structure
    local data_dir
    data_dir=$(find "$extract_to" -type d -name "data" | head -n 1)
    
    if [[ -n "$data_dir" && "$data_dir" != "$extract_to/data" ]]; then
        echo "Moving $data_dir to $extract_to/data ..."
        mv "$data_dir" "$extract_to/data"
    fi

    # Remove empty intermediate folders
    find "$extract_to" -type d -empty -delete
}


##############################################################################
# Function to download and extract a single year
##############################################################################
process_year() {
    local year="$1"
    # "declare -n" to reference the array named "URLS_$year"
    declare -n month_map="URLS_${year}"

    echo "=== Processing year $year ==="
    local workspace_dir="data"
    mkdir -p "$workspace_dir"

    local monthly_tar_files=()

    # If the array is empty (no months), skip
    if [[ ${#month_map[@]} -eq 0 ]]; then
        echo "No month->URL mappings found for year $year. Skipping."
        return
    fi

    # Sort months for a predictable order
    local months_sorted
    IFS=$'\n' months_sorted=($(sort -n <<<"${!month_map[*]}"))
    unset IFS

    for month in "${months_sorted[@]}"; do
        local url="${month_map[$month]}"
        printf -- "- Month %02d\n" "$month"
        local month_str="${year}$(printf "%02d" "$month")"

        # 1) Download
        local tar_gz_filename="${month_str}.tar.gz"
        local tar_gz_filepath="${workspace_dir}/${tar_gz_filename}"
        download_file "$url" "$tar_gz_filepath"

        # 2) Create folder for extraction
        local extract_target_dir="${workspace_dir}/${month_str}"
        mkdir -p "$extract_target_dir"

        # 3) Extract
        extract_tar_gz "$tar_gz_filepath" "$extract_target_dir"
    done

    echo "Year $year download complete."
    echo
}

##############################################################################
# main
##############################################################################
main() {
    local years=("2023")
    for y in "${years[@]}"; do
        process_year "$y"
    done
}

# Call main if script is invoked directly
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main
fi
