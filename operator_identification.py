#!/usr/bin/python3
import argparse
import logging
import os
import polars as pl


from constants import (
    EXCLUDE_UNITS_FILE,
    FILE_LIST,
    OPERATOR_TECHNOLOGY_MAPPING,
    OPERATOR_TECHNOLOGY_YEAR,
    UNIT_OPERATOR_MAP_FILE,
    UNIT_PROFILE_FILE,
)
from helpers import group_by_targets, create_unit_timezone_map


# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    filename="./logging/operator_identification_run.log",
    filemode="a",
    format="%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)

# -----------------------------------------------------------------------------
# Helper Classes and Functions
# -----------------------------------------------------------------------------
def get_timezone_offset(unit_timezone_map: dict, unit_id) -> list:
    return unit_timezone_map.get(unit_id, [None, None])

# -----------------------------------------------------------------------------
# Unit Profile and Timezone Mapping
# -----------------------------------------------------------------------------

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
    
def avg_range(val):
    # Check if the value is a string
    if isinstance(val, str):
        if '[' in val:
            val = val[1:-1]  # removes the chars '[', ']' at start and end
            l = float(val.split("-")[0])
            h = float(val.split("-")[1]) if len(val.split("-")) == 2 else 0
            return (l + h) / 2 if h != 0 else l
        elif is_number(val):
            return float(val)
        else:
            return None
    else:
        # Otherwise, return the value as is (or cast to float if needed)
        return float(val)


def get_combined_unit_profile(year: str) -> pl.DataFrame:
    """
    Combine and clean unit profile data for a given year.
    """

    if year == '2022' or year == '2023':
        profile = pl.read_excel(UNIT_PROFILE_FILE[year])
    else:
        profile = pl.read_csv(UNIT_PROFILE_FILE[year], ignore_errors=True)
    profile = profile.rename({col: col.lower().replace(' ', '_') for col in profile.columns})
    profile = profile.with_columns(pl.col("unit_id").cast(pl.Int64).alias("unit_id"))
    profile = profile.with_columns(pl.lit("fcc").alias("validation_type"))

    if EXCLUDE_UNITS_FILE.get(year, False):
        exclude_df = pl.read_excel(EXCLUDE_UNITS_FILE.get(year))
        exclude_df = exclude_df.rename({
            exclude_df.columns[1]: "ISP",
            exclude_df.columns[0]: "Unit ID"
        })
        exclude_df = exclude_df.rename({col: col.lower().replace(' ', '_') for col in exclude_df.columns})

        if year == "2012":
            exclude_df = exclude_df.with_columns([
                pl.col("download").str.replace("Mbit/s", ""),
                pl.col("upload").str.replace("Mbit/s", "")
            ])
        
        exclude_df = exclude_df.with_columns([
            pl.when(pl.col(col) == "Unknown")
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col, dtype in exclude_df.schema.items() if dtype == pl.Utf8
        ])

        profile = pl.concat([profile, exclude_df], how='diagonal_relaxed').unique(subset=["unit_id"])
        profile = profile.with_columns(pl.col("technology").fill_null("").alias("technology"))

    # Process Download and Upload columns if present
    if profile.schema['download'] ==  pl.Utf8 or profile.schema['upload'] ==  pl.Utf8:
        if "download" in profile.columns and "upload" in profile.columns:
            for col in ["download", "upload"]:
                profile = profile.with_columns(pl.col(col).map_elements(avg_range, return_dtype=float).alias(col))

                    
    if year == "2017":
        for col in ["timezone_offset", "timezone_offset_dst"]:
            if col in profile.columns:
                profile = profile.with_columns(pl.col(col).str.replace(" hr", ""))
    elif year == "2011":
        unit_timezone_map = create_unit_timezone_map(year)
        # Use the mapping to update timezone columns based on "Unit ID"
        # TODO: Vectorize
        profile = profile.with_columns([
            pl.col("unit_id").map_elements(lambda uid: get_timezone_offset(unit_timezone_map, uid)[0]).alias("timezone_offset"),
            pl.col("unit_id").map_elements(lambda uid: get_timezone_offset(unit_timezone_map, uid)[1]).alias("timezone_offset_dst")
        ])
        profile = profile.with_columns([
            pl.col("timezone_offset").fill_nan(None).cast(pl.Int64),
            pl.col("timezone_offset_dst").fill_nan(None).cast(pl.Int64)
        ])

    if {"timezone_offset", "timezone_offset_dst"}.issubset(set(profile.columns)):
        for col in ["timezone_offset", "timezone_offset_dst"]:
            profile = profile.with_columns(pl.col(col).cast(pl.Int64))

    # Clean ISP names (for 2020, 2018, 2016, etc.)
    if "isp" in profile.columns:
        profile = profile.with_columns(
            pl.col("isp")
            .str.strip_chars()
            .str.replace("Verizon DSL", "Verizon")
            .str.replace("Verizon Fiber", "Verizon")
            .str.replace("Verizon Wireless", "Verizon")
            .str.replace("Verizon Business", "Verizon")
            .str.replace("Hawaiian Telcom", "Hawaiian Telecom")
            .str.replace("AT&T DSL", "AT&T")
            .str.replace("AT&T IPBB", "AT&T")
            .str.replace("Cincinnati Bell DSL", "Cincinnati Bell")
            .str.replace("Cincinnati Bell Fiber", "Cincinnati Bell")
            .str.replace("Frontier DSL", "Frontier")
            .str.replace("Frontier Fiber", "Frontier")
            .str.replace("TWC", "TimeWarner")
            .str.replace("Time Warner Cable", "TimeWarner")
            .str.replace("Oceanic TWC", "TimeWarner")
            .str.replace("Miscellaneous", "")
            .alias("isp")
        )  
    
    # Clean Technology column
    if "technology" in profile.columns:
        profile = profile.with_columns(
            pl.col("technology")
            .str.strip_chars()
            .str.replace("CABLE", "Cable")
            .str.replace("CABLE - BUSINESS", "Cable")
            .str.replace("FIBER", "Fiber")
            .str.replace("SATELLITE", "Satellite")
            .str.replace("SAT", "Satellite")
            .str.replace("WIRELESS", "Wireless")
            .str.replace("REMOVE", "")
            .str.replace("MISC", "")
            .str.replace("Unknown", "")
            .alias("technology")
        )

     # Fill missing Download, Upload, and validation_type values
    profile = profile.with_columns(pl.col("download").fill_null(0))
    profile = profile.with_columns(pl.col("upload").fill_null(0))
    profile = profile.with_columns(pl.col("validation_type").fill_null("bmc"))

    profile = profile.rename({'isp': 'operator_name', 'technology': 'operator_technology',
                              'download': 'download_speed', 'upload': 'upload_speed'})
    
    profile = profile.select(['unit_id', 'operator_name', 'operator_technology', 
                              'download_speed', 'upload_speed', 'validation_type', 
                              'timezone_offset', 'timezone_offset_dst'])
    
    return profile


# -----------------------------------------------------------------------------
# Unit ID and Operator Mapping
# -----------------------------------------------------------------------------
def get_all_unit_ids(file_path_list: list) -> set:
    """
    Read all parquet files from the provided list and return the set of all unique unit IDs.
    """
    all_unit_ids = set()
    for file_path in file_path_list:
        if os.path.isfile(file_path):
            df = pl.scan_parquet(file_path)
            unit_ids = df.select("unit_id").collect().unique().to_series().to_list()
            all_unit_ids.update(unit_ids)
    return all_unit_ids


def get_operator_name(file_path_list: list, year: str) -> pl.LazyFrame:
    """
    Process each parquet file and return a Polars DataFrame mapping each unit_id to a cleaned operator
    along with additional placeholder values 
    """
    # List to store per-file DataFrames (only unit_id and cleaned operator)
    dfs = []
    
    for file_path in file_path_list:
        if not os.path.isfile(file_path):
            continue
        
        # Read the file lazily and process with group_by_targets
        data = pl.scan_parquet(file_path)
        data = group_by_targets(data, year)
        
        # Identify and report "no_operator" targets
        no_operator_df = data.filter(pl.col('operator') == 'no_operator')
        no_operator_targets = no_operator_df.select("target").collect().unique().to_series().to_list()
        if no_operator_targets:
            logging.debug(f"The remaining targets for file: {file_path} are: {no_operator_targets}")
        
        # Filter out unwanted operator rows
        data = data.filter(~pl.col('operator').str.contains("off-net"))
        data = data.filter(pl.col('operator') != 'no_operator')
        
        # Collect the data and clean the operator column.
        # data = data.collect()
        # First, split and extract the first element, and alias it as "operator_name"
        data = data.with_columns(
            pl.col("operator")
            .str.split("(")
            .list.first()
            .str.strip_chars()
            .alias("operator_name")
        )

        data = data.with_columns(pl.col("unit_id").cast(pl.Int64).alias("unit_id"))

        # Keep only unit_id and the cleaned operator
        dfs.append(data.select(["unit_id", "operator_name"]))
    
    if not dfs:
        # Return an empty DataFrame with the expected columns if no files were processed
        return pl.LazyFrame({
            "unit_id": [],
            "operator_name": [],
            "operator_technology": [],
            "download_speed": [],
            "upload_speed": [],
            "validation_type": [],
            "timezone_offset": [],
            "timezone_offset_dst": [],
        })
    
    
    # Combine results from all files.
    final_df = pl.concat(dfs)
    final_df = final_df.unique(subset=["unit_id"])

    # Add extra placeholder values for certain columns to be populated in the future.
    final_df = final_df.with_columns([
        pl.lit("").alias("operator_technology"),
        pl.lit(0).alias("download_speed"),
        pl.lit(0).alias("upload_speed"),
        pl.lit("bmc").alias("validation_type"),
        pl.lit(0).alias("timezone_offset"),
        pl.lit(0).alias("timezone_offset_dst"),
    ])
    
    # Reorder columns if necessary.
    final_df = final_df.select(["unit_id", "operator_name", "operator_technology", 
                                "download_speed", "upload_speed", "validation_type", 
                                "timezone_offset", "timezone_offset_dst"])
    
    return final_df


def get_unit_operator_map(data_folder: str) -> None:
    """
    Build the complete mapping of unit IDs to operator details.
    This includes classification based on target hostname, supplementing with unit profile data,
    and refining the operator technology using download speed from curr_httpgetmt.
    Finally, the mapping is written to a parquet file.
    """
    year = os.path.basename(data_folder)[:4]
    if os.path.basename(data_folder) == '201602':
        FILE_LIST.remove("curr_udplatency.parquet")
    file_path_list = [os.path.join(data_folder, file) for file in FILE_LIST]
    
    all_unit_ids = get_all_unit_ids(file_path_list)
    logging.debug(f"Starting operator_name classification process for {os.path.basename(data_folder)}")
    
    unit_id_operator_mapping_df = get_operator_name(file_path_list, year)
     
    logging.debug(f"Total count of all ids: {len(all_unit_ids)}")
    classified_unit_ids = set(unit_id_operator_mapping_df.collect().unique().to_series().to_list())
    logging.debug(f"Count of classified ids: {len(classified_unit_ids)}")
    unclassified_unit_ids = all_unit_ids - classified_unit_ids
    logging.debug(f"Count of unclassified ids: {len(unclassified_unit_ids)}")
    
    logging.debug("Using unit_profile file to further classify the remaining units")
    unit_profile = get_combined_unit_profile(year)
    unclassified_uid_df = unit_profile.filter(pl.col("unit_id").is_in(list(unclassified_unit_ids)))
    
    profile_units = set(unit_profile["unit_id"].to_list())
    not_present = classified_unit_ids - profile_units
    logging.debug(f"Count of unit_ids classified based on target server but not present in unit-profile file: {len(not_present)}")
    
    # Ensure required columns exist
    for col in ["download_speed", "upload_speed", "timezone_offset", "timezone_offset_dst"]:
        if col not in unclassified_uid_df.columns:
            unclassified_uid_df = unclassified_uid_df.with_columns(pl.lit(0).alias(col))
            unit_profile = unit_profile.with_columns(pl.lit(0).alias(col))
    
    # Update unit_id_operator_mapping_df with values from unclassified_uid_df
    # First remove all the placeholder rows that have values in unclassified_uid_df
    unit_id_operator_mapping_df = unit_id_operator_mapping_df.filter(~pl.col("unit_id").is_in(unclassified_uid_df["unit_id"])) 
    # Now add the rows with values
    unit_id_operator_mapping_df = pl.concat([unit_id_operator_mapping_df, unclassified_uid_df.lazy()], how='vertical_relaxed')

    # Update mapping for units already classified (to add speeds and technology)
    # Update unit_id_operator_mapping_df with values from unit_profile
    unit_ids_series = (
        unit_id_operator_mapping_df
        .select("unit_id")
        .collect()
        .to_series()
    )
    units_to_add = unit_profile.filter(pl.col("unit_id").is_in(unit_ids_series))
    unit_id_operator_mapping_df = unit_id_operator_mapping_df.filter(~pl.col("unit_id").is_in(units_to_add.select("unit_id")) ) 
    unit_id_operator_mapping_df = pl.concat([unit_id_operator_mapping_df, units_to_add.lazy()], how='vertical_relaxed')


    logging.debug(f"Total count of all ids: {len(all_unit_ids)}")
    classified_unit_ids = set(unit_id_operator_mapping_df.collect().unique().to_series().to_list())
    logging.debug(f"Count of classified ids: {len(classified_unit_ids)}")
    unclassified_unit_ids = all_unit_ids - classified_unit_ids
    unclassified_unit_ids_count = len(unclassified_unit_ids)
    unclassified_unit_ids_count += (
        unit_id_operator_mapping_df
        .filter(pl.col("operator_name") == "")
        .collect()
        .height
    )
    logging.debug(f"Final count of unclassified ids: {unclassified_unit_ids_count}")
    
    logging.debug("Next, operator_technology classification")
    operator_mapping_dict = OPERATOR_TECHNOLOGY_MAPPING[year]
    unit_id_operator_mapping_df = unit_id_operator_mapping_df.with_columns(
        pl.when(pl.col("operator_name").is_in(operator_mapping_dict))
        .then(pl.col("operator_name").replace(operator_mapping_dict))
        .otherwise(pl.col("operator_technology"))
        .alias("operator_technology")
    )
    
    logging.debug("Using unit_profile file to classify remaining units based on Technology")
    # Select only the necessary columns from unit_profile to avoid duplicating unwanted data.
    profile_update = unit_profile.select(["unit_id", "operator_name", "operator_technology"]).rename({
        "operator_name": "operator_name_profile",
        "operator_technology": "operator_technology_profile"
    }).lazy()

    # Left join the mapping with the profile update on "unit_id"
    unit_id_operator_mapping_df = unit_id_operator_mapping_df.join(profile_update, on="unit_id", how="left")

    # Update the operator_technology column:
    # If operator_technology_profile is not empty and the operator_name from the mapping matches the operator_name from the profile,
    # then update operator_technology with operator_technology_profile, else keep the original operator_technology.
    unit_id_operator_mapping_df = unit_id_operator_mapping_df.with_columns(
        pl.when((pl.col("operator_technology_profile") != "") & (pl.col("operator_name") == pl.col("operator_name_profile")))
        .then(pl.col("operator_technology_profile"))
        .otherwise(pl.col("operator_technology"))
        .alias("operator_technology")
    )

    unit_id_operator_mapping_df = unit_id_operator_mapping_df.drop(["operator_name_profile", "operator_technology_profile"])
    unclassified_tech_count = unit_id_operator_mapping_df.filter(pl.col("operator_technology") == "").collect().height
    logging.debug(f"Count of unclassified technology: {unclassified_tech_count}")
    
    logging.debug("Using curr_httpgetmt to identify technology for ISPs with multiple technology types")
    download_file = os.path.join(data_folder, "curr_httpgetmt.parquet")
    download_data = pl.scan_parquet(download_file) if os.path.isfile(download_file) else None
    
    for operator, cutoff_speed in OPERATOR_TECHNOLOGY_YEAR[year]:
        if download_data is None:
            break
        filtered_df = unit_id_operator_mapping_df.filter(pl.col("operator_name") == operator)

        operator_speeds = download_data.join(
            filtered_df.select("unit_id").lazy(),  # still a LazyFrame
            on="unit_id",
            how="inner"
        )

            
        download_info = operator_speeds.group_by("unit_id") \
            .agg(pl.col("bytes_sec").mean().alias("bytes_sec_mean")) \
            .with_columns(((pl.col("bytes_sec_mean") * 8) / (1024 * 1024)).round(2).alias("bytes_sec"))
        download_info = download_info.select(["unit_id", "bytes_sec"])

        # Left join the mapping dataframe with download_info on "unit_id"
        unit_id_operator_mapping_df = unit_id_operator_mapping_df.lazy().join(download_info, on="unit_id", how="left")

        # Update "technology" and "operator" columns conditionally:
        # - Only update rows where the existing "technology" is empty and there is a matching bytes_sec value.
        # - If bytes_sec <= cutoff_speed, set "technology" to "DSL", else set to "Fiber".
        # - Also update "operator" to the provided operator variable.
        unit_id_operator_mapping_df = unit_id_operator_mapping_df.with_columns(
            pl.when(
                (pl.col("operator_technology") == "") & 
                pl.col("bytes_sec").is_not_null() & 
                (pl.col("bytes_sec") <= pl.lit(cutoff_speed))
            )
            .then(pl.lit("DSL"))
            .when(
                (pl.col("operator_technology") == "") & 
                pl.col("bytes_sec").is_not_null() & 
                (pl.col("bytes_sec") > pl.lit(cutoff_speed))
            )
            .then(pl.lit("Fiber"))
            .otherwise(pl.col("operator_technology"))
            .alias("operator_technology")
        )

        unit_id_operator_mapping_df = unit_id_operator_mapping_df.drop("bytes_sec").collect()

    unclassifed_tech_df = unit_id_operator_mapping_df.filter(pl.col("operator_technology") == "")
    unclassified_tech_count = unclassifed_tech_df.height
    unclassified_tech_unit_ids = unclassifed_tech_df['unit_id'].unique().to_list()
    logging.debug(f"Final count of unclassified technology: {unclassified_tech_count}")
    logging.debug(f"Some of the unclassified technology unit IDs are: {unclassified_tech_unit_ids[:10]}")
    

    # Write the final mapping to a parquet file
    file_path = os.path.join(data_folder, UNIT_OPERATOR_MAP_FILE)
    logging.debug(f"Writing to {file_path} with shape {unit_id_operator_mapping_df.shape}")
    unit_id_operator_mapping_df.write_csv(file_path)
    
    # Log aggregated ISP stats
    agg_info = unit_id_operator_mapping_df.group_by(["operator_name", "operator_technology"]).agg(pl.count("unit_id").alias("count"))

    total_count = agg_info.select(pl.col("count")).sum().item()
    total_df = pl.DataFrame([["Total", "", total_count]], schema=["operator_name", "operator_technology", "count"], orient="row")
    agg_info = pl.concat([agg_info, total_df], how='vertical_relaxed')
    logging.info(f"The ISP stats for current month is:\n{agg_info.to_pandas().to_markdown()}")
    
    return None

# -----------------------------------------------------------------------------
# Main Function
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-folder", type=str, help="Path to data folder")
    args = parser.parse_args()
    logging.info(f"Generating unit_id to operator mapping file for the month of {args.data_folder}")
    get_unit_operator_map(args.data_folder)


if __name__ == "__main__":
    main()
