import argparse
import logging
import os
import polars as pl

# Import constants and helper functions
from constants import (
    INGEST_FILE_LIST,
    FILTER_MAP,
    EXCLUDE_UNITS_FILE,
    UNIT_OPERATOR_MAP_FILE,
    OPERATOR_HOSTNAME_MAPPING
)
from helpers import create_unit_timezone_map


logging.basicConfig(
    filename="./logging/filter.log",
    filemode="a",
    format="%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


def filter_data(data: pl.LazyFrame, file_name: str, year: str, exclude_units_list: list) -> pl.LazyFrame:

    filter_list = FILTER_MAP.get(file_name)
    logging.info(f"Filter list for file {file_name}: {filter_list}")

    if year == "2011" and file_name in ["curr_ulping.csv", "curr_dlping.csv"] and 'less_than_50_samples' in filter_list:
        filter_list.remove('less_than_50_samples')
    

    if exclude_units_list and "exclude_units" in filter_list:
        if year == '2020':
            data = data.with_columns(pl.col("unit_id").cast(pl.Int64))
        data = data.filter(~pl.col("unit_id").is_in(exclude_units_list))
        logging.info(f"Removed exclude units.")

    if "zeros" in filter_list:
        for col in data.columns:
            if col not in (["target", "dtime", "failures"]):
                data = data.filter(pl.col(col) != 0)

    if "delta_rtt>300ms" in filter_list:
        data = data.filter((pl.col("rtt_max") - pl.col("rtt_min")) <= 300000)
    
    if "rtt_min<0.05ms" in filter_list:
        data = data.filter(pl.col("rtt_min") >= 50)
    
    # if "less_than_50_samples" in filter_list:
    #     data = data.filter((pl.col("successes") + pl.col("failures")) >= 50)

    if "packet_loss_10" in filter_list:
        condition = pl.col("failures") / (pl.col("failures") + pl.col("successes")) <= 0.1
        data = data.filter(condition)

    if "tests_without_jitter" in filter_list:
        condition = (pl.col("jitter_up") <= 0) | (pl.col("jitter_down") <= 0)
        data = data.filter(~condition)

    if "speed_low_pass_filter" in filter_list:
        data = data.filter(pl.col("bytes_sec") <= 123207680.0)
        logging.info(f"Removed values higher than 940 Mbps.")
    
    if "speed_high_pass_filter" in filter_list:

        schema = data.collect_schema()
        col_type = schema.get("bytes_sec_interval", False)

        if not col_type:
            data = data.with_columns(pl.lit(1).alias("bytes_sec_interval")) # 1 to escape the filter     
        elif col_type == pl.Utf8:
            data = data.with_columns(
                pl.col("bytes_sec_interval")
                .str.replace("NULL", "1")
                .cast(pl.Int32)
            )
        
        data = data.filter((pl.col("bytes_sec") > 0) & (pl.col("bytes_sec_interval") > 0))
        logging.info(f"Removed values lower than or equal to 0.")

    if "remove_failed_tests" in filter_list:
        data = data.filter(pl.col("failures") != 1)
        logging.info(f"Removed failed tests.")
    
    return data


def update_date(data: pl.LazyFrame, unit_timezone_map: dict) -> pl.LazyFrame:
    """
    Update the date column to the local timezone and compute the time category.
    """
    # --- Fill null values in 'timezone_offset' with the mapped values ---

    # Create a lazy frame with the mapping of unit IDs to timezone
    tz_map_df = pl.LazyFrame({
        "unit_id": list(unit_timezone_map.keys()),
        "map_offset": [unit_timezone_map[uid][0] for uid in unit_timezone_map],
        "map_offset_dst": [unit_timezone_map[uid][1] for uid in unit_timezone_map],
    })

    # Join the lazy frame with the mapping (left join on 'unit_id')
    data = data.join(tz_map_df, on="unit_id", how="left")

    # Fill missing timezone offsets with the mapped values.
    # The new columns 'tz_offset' and 'tz_offset_dst' will be used for further calculations.
    data = data.with_columns([
        pl.coalesce(["timezone_offset", "map_offset"]).fill_nan(0).alias("tz_offset"),
        pl.coalesce(["timezone_offset_dst", "map_offset_dst"]).fill_nan(0).alias("tz_offset_dst")
    ])

    # --- Parse the 'dtime' column into a datetime type ---
    data = data.with_columns(
        pl.col("dtime").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S").alias("dtime_parsed")
    )

    # --- Compute the local datetime and time category ---
    # Ensure all dates are from 2007 onward.
    data = data.filter(pl.col("dtime_parsed").dt.year() >= 2007)

    # Extract the year from the parsed datetime.
    data = data.with_columns(pl.col("dtime_parsed").dt.year().alias("year"))

    # --- Compute DST boundaries ---
    # DST starts on the second Sunday in March at 2:00 AM.
    # We compute a base date: "year-03-08 02:00:00" and then add (6 - weekday) days.
    data = data.with_columns([
        # Build the base start date for DST.
        ((pl.col("year").cast(pl.Utf8) + "-03-08 02:00:00")
         .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")).alias("base_start"),
        # Build the base end date for DST ("year-11-01 02:00:00")
        ((pl.col("year").cast(pl.Utf8) + "-11-01 02:00:00")
         .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")).alias("base_end"),
    ])

    # Compute DST start: add (6 - weekday) days (converted to seconds) to the base start.
    data = data.with_columns([
        (
                pl.col("base_start") +
                ((pl.lit(6) - pl.col("base_start").dt.weekday()) * 86400 * 1000)
                .cast(pl.Duration("ms"))
        ).alias("dst_start"),
        (
                pl.col("base_end") +
                ((pl.lit(6) - pl.col("base_end").dt.weekday()) * 86400 * 1000)
                .cast(pl.Duration("ms"))
        ).alias("dst_end"),
    ])

    # Flag rows where dtime_parsed is in DST: [dst_start, dst_end)
    data = data.with_columns(
        ((pl.col("dtime_parsed") >= pl.col("dst_start")) &
         (pl.col("dtime_parsed") < pl.col("dst_end")))
        .alias("in_dst")
    )

    # --- Adjust Local Time ---
    # For each row, choose tz_offset_dst if in DST; otherwise, use tz_offset.
    # Multiply by 3600 to convert hours to seconds, then cast to a Duration.
    data = data.with_columns(
        pl.when(pl.col("in_dst"))
        .then(pl.col("tz_offset_dst") * 3600 * 1000)
        .otherwise(pl.col("tz_offset") * 3600 * 1000)
        .fill_nan(0)
        .alias("offset_seconds")
    )

    # Apply the offset to get the local datetime.
    data = data.with_columns(
        (pl.col("dtime_parsed") + pl.col("offset_seconds").cast(pl.Duration("ms")))
        .alias("local_dt")
    )

    data = data.with_columns(
        pl.coalesce(["local_dt", "dtime_parsed"]).alias("local_dt")
    ) 

    # Format the localized datetime as a string.
    data = data.with_columns(
        pl.col("local_dt").dt.strftime("%Y-%m-%d %H:%M:%S").alias("dtime_local")
    )

    # --- Compute Time Category ---
    # Weekdays (Monday=0 to Friday=4): if local time is between 19:00 and 22:59:59 -> "peak-hours",
    # otherwise "off-peak-hours". Weekends are labeled as "sat-sun".
    data = data.with_columns(
        pl.when(
            (pl.col("local_dt").dt.weekday() < 6) &
            (pl.col("local_dt").dt.hour() >= 19) &
            (pl.col("local_dt").dt.hour() < 23)
        ).then(pl.lit("peak-hours"))
        .otherwise(
            pl.when(pl.col("local_dt").dt.weekday() < 6)
            .then(pl.lit("off-peak-hours"))
            .otherwise(pl.lit("sat-sun"))
        ).alias("time_category")
    )


    # Drop the intermediate columns.
    data = data.drop([
        "map_offset", "map_offset_dst", "tz_offset", "tz_offset_dst",
        "dtime_parsed", "year", "base_start", "base_end",
        "dst_start", "dst_end", "in_dst", "offset_seconds", "local_dt",
        "timezone_offset", "timezone_offset_dst"
    ])

    return data


def classify_on_off_net(data: pl.LazyFrame, year: str) -> pl.LazyFrame:
    # Build a vectorized "operator" column using an if-elif chain.
    operator_expr = pl.lit("off-net")  # default value
    for operator_name, hostname_regex in OPERATOR_HOSTNAME_MAPPING[year].items():
        operator_expr = pl.when(pl.col("target").str.contains(pattern=hostname_regex)).then(pl.lit(operator_name)).otherwise(operator_expr)
    
    data = data.with_columns(operator_expr.alias("operator"))
    
    # Now assign test_type: if the operator string contains "on-net", mark as on-net;
    # otherwise (or if it’s "off-net") it remains off-net.
    data = data.with_columns(
        pl.when(pl.col("operator").str.contains("on-net"))
          .then(pl.lit("on-net"))
          .otherwise(pl.lit("off-net"))
          .alias("test_type")
    )
    
    # Optionally, drop the operator column if you don't need it:
    # data = data.drop(["operator"])
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-folder", type=str, help="Path to folder containing raw data")
    parser.add_argument("--processed-folder", type=str, help="Path to folder to store filtered data")
    args = parser.parse_args()

    file_path_list = [os.path.join(args.data_folder, file+'.parquet') for file in INGEST_FILE_LIST]
    month = args.data_folder.split('/')[-1]
    year = month[:4]
    logging.info(f"Year: {year}")

    # Read the unit mapping CSV with proper null handling.
    unit_mapping_path = os.path.join(args.data_folder, UNIT_OPERATOR_MAP_FILE)
    unit_mapping = pl.scan_csv(unit_mapping_path, null_values=["NULL"])

    # Create a mapping of unit IDs to timezone offsets.
    unit_timezone_map = create_unit_timezone_map(year)

    # Read excluded units
    exclude_units_list = None
    if not EXCLUDE_UNITS_FILE.get(year) or not os.path.isfile(EXCLUDE_UNITS_FILE.get(year)):
        logging.info(f"Exclude units file not found for year {year}. Skipping filter.")
    else:
        logging.info(f"Reading exclude units file for year {year}")
        exclude_df = pl.read_excel(EXCLUDE_UNITS_FILE.get(year))
        if year == '2011':
            exclude_df = exclude_df.rename({'UnitID': 'unit_id'})
        exclude_df = exclude_df.rename({col: col.lower().replace(' ', '_') for col in exclude_df.columns})
        exclude_units_list = exclude_df["unit_id"].unique().to_list()

    for file_path in file_path_list:
    
        if not os.path.isfile(file_path):
            logging.info(f"File {file_path} is not present for year {year}.")
            continue
        
        file_name = file_path.split("/")[-1][:-8]
        if file_name == 'curr_udpjitter' and month in ['201102', '201103', '201104', '201105']:
            logging.info(f"Skipping {file_name} for {month} due to improper schema/values.")
            continue

        logging.info(f"File: {file_name}")
        logging.info(f"File path: {file_path}")

        df = pl.scan_parquet(file_path) #,ignore_errors=True, infer_schema_length=10000, truncate_ragged_lines=True)
        logging.info(f"Data from file {file_path} loaded to Polars.")

        df = filter_data(data=df, file_name=file_name, year=year, exclude_units_list=exclude_units_list)
        logging.info(f"Data filtered for file {file_name}.")
        
        df = df.join(unit_mapping, on="unit_id", how="left")
        logging.info(f"Unit mapping applied to file {file_name}.")

        df = update_date(df, unit_timezone_map)
        logging.info(f"Date updated for file {file_name}.")

        df = classify_on_off_net(df, year)
        logging.info(f"On/Off net classification done for file {file_name}.")

        write_file_path = os.path.join(args.processed_folder, f"{file_name}.parquet")
        df.sink_parquet(write_file_path)
        logging.info(f"Filtered data written to {write_file_path}")
            
    logging.info("Filter process completed.")


if __name__ == "__main__":
    main()