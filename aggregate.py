import argparse
import logging
import os
import polars as pl

# Import constants
from constants import INGEST_FILE_LIST

# Logging file
logging.basicConfig(
    filename="./logging/aggregate.log",
    filemode="a",
    format="%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


def aggregate_throughput(df: pl.LazyFrame, year: str) -> pl.LazyFrame:

    if year == '2020':
        df = df.with_columns(
            pl.col("bytes_sec").cast(pl.Float64).alias("bytes_sec"),
            pl.col("bytes_sec_interval").cast(pl.Float64).alias("bytes_sec_interval")
        )
    
    df = df.with_columns(((pl.col("bytes_sec") * 8 * 100 / (1024*1024))/pl.col("download_speed")).alias("percent_achieved_download"))
    df = df.with_columns(((pl.col("bytes_sec") * 8 * 100 / (1024*1024))/pl.col("upload_speed")).alias("percent_achieved_upload"))
    
    # Group by unit_id, year, and month, and aggregate columns as required
    aggregated_df = df.group_by(["year", "month", "unit_id"]).agg([
        pl.col("operator_name").last().alias("operator_name"),
        pl.col("operator_technology").last().alias("operator_technology"),
        pl.col("download_speed").last().alias("adv_download_speed"),
        pl.col("upload_speed").last().alias("adv_upload_speed"),
        pl.col("bytes_sec").mean().alias("bytes_sec"),
        pl.col("bytes_sec_interval").mean().alias("bytes_sec_interval"),
        pl.col("percent_achieved_download").mean().alias("percent_achieved_download"),
        pl.col("percent_achieved_upload").mean().alias("percent_achieved_upload")
    ])

    aggregated_df = aggregated_df.with_columns(
        (pl.col("bytes_sec") * 8 / (1024*1024)).alias("throughput"),
        (pl.col("bytes_sec_interval") * 8 / (1024*1024)).alias("throughput_interval")
    )
    
    return aggregated_df


def aggregate_latency(df: pl.LazyFrame, year: str) -> pl.LazyFrame:

    # Group by unit_id, year, and month, and aggregate columns as required
    aggregated_df = df.group_by(["year", "month", "unit_id"]).agg([
        pl.col("operator_name").last().alias("operator_name"),
        pl.col("operator_technology").last().alias("operator_technology"),
        pl.col("download_speed").last().alias("adv_download_speed"),
        pl.col("upload_speed").last().alias("adv_upload_speed"),
        (pl.col("rtt_avg").mean()/1000).alias("rtt_avg"),
        (pl.col("rtt_max").mean()/1000).alias("rtt_max"),
        (pl.col("rtt_min").mean()/1000).alias("rtt_min"),
        (pl.col("rtt_std").mean()/1000).alias("rtt_std"),
    ])
    
    return aggregated_df


def aggregate_jitter(df: pl.LazyFrame, year: str) -> pl.LazyFrame:

    df = df.with_columns(
        (((pl.col("packets_up_sent") - pl.col("packets_up_recv"))/pl.col("packets_up_sent")) * 100).alias("packet_loss_up_pct")
    )

    df = df.with_columns(
        (((pl.col("packets_down_sent") - pl.col("packets_down_recv"))/pl.col("packets_down_sent")) * 100).alias("packet_loss_down_pct")
    )

    # Group by unit_id, year, and month, and aggregate columns as required
    aggregated_df = df.group_by(["year", "month", "unit_id"]).agg([
        pl.col("operator_name").last().alias("operator_name"),
        pl.col("operator_technology").last().alias("operator_technology"),
        pl.col("download_speed").last().alias("adv_download_speed"),
        pl.col("upload_speed").last().alias("adv_upload_speed"),
        pl.col("packet_size").mean().alias("packet_size"),
        pl.col("packets_up_sent").mean().alias("packets_up_sent"),
        pl.col("packets_down_sent").mean().alias("packets_down_sent"),
        pl.col("packets_up_recv").mean().alias("packets_up_recv"),
        pl.col("packets_down_recv").mean().alias("packets_down_recv"),
        (pl.col("jitter_up").mean()/1000).alias("jitter_up"),  # Convert to ms
        (pl.col("jitter_down").mean()/1000).alias("jitter_down"),  # Convert to ms
        pl.col("latency").mean().alias("latency"),
        pl.col("packet_loss_up_pct").mean().alias("packet_loss_up_pct"),
        pl.col("packet_loss_down_pct").mean().alias("packet_loss_down_pct")
    ])

    aggregated_df = aggregated_df.with_columns(
        pl.when(pl.col("packet_loss_up_pct") < 0.4)
        .then(pl.lit("<0.4%"))
        .when((pl.col("packet_loss_up_pct") >= 0.4) & (pl.col("packet_loss_up_pct") <= 1))
        .then(pl.lit("0.4%-1%"))
        .otherwise(pl.lit(">1%"))
        .alias("packet_loss_up_bin")
    )
    
    aggregated_df = aggregated_df.with_columns(
        pl.when(pl.col("packet_loss_down_pct") < 0.4)
        .then(pl.lit("<0.4%"))
        .when((pl.col("packet_loss_down_pct") >= 0.4) & (pl.col("packet_loss_down_pct") <= 1))
        .then(pl.lit("0.4%-1%"))
        .otherwise(pl.lit(">1%"))
        .alias("packet_loss_down_bin")
    )

    return aggregated_df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-folder", type=str, help="Path to folder containing processed data")
    parser.add_argument("--month", type=str, help="Month and Year in format YYYYMM")
    args = parser.parse_args()  

    file_path_list = [os.path.join(args.processed_folder, file+'.parquet') for file in INGEST_FILE_LIST]
    year = args.month[:4]
    logging.info(f"Year: {year}")

    for file_path in file_path_list:

        if not os.path.isfile(file_path):
            logging.info(f"File {file_path} is not present for year {year}.")
            continue
        
        file_name = file_path.split("/")[-1][:-8]
        if file_name == 'curr_udpjitter' and args.month in ['201102', '201103', '201104', '201105']:
            logging.info(f"Skipping {file_name} for {args.month} due to improper schema/values.")
            continue
        
        logging.info(f"File: {file_name}")
        
        # Read the parquet file lazily
        df = pl.scan_parquet(file_path)
        logging.info(f"Data from file {file_path} loaded to Polars.")

        # Convert the dtime column to a datetime type
        df = df.with_columns(pl.col("dtime").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S").alias("dtime"))

        # Extract the year and month from the dtime column
        df = df.with_columns([
            pl.col("dtime").dt.year().alias("year"),
            pl.col("dtime").dt.month().alias("month")
        ])

        if file_name in ['curr_ulping', 'curr_dlping', 'curr_udplatency']:
            aggregated_df = aggregate_latency(df, year)
        elif file_name in ['curr_httpgetmt', 'curr_httppostmt']:
            aggregated_df = aggregate_throughput(df, year)
        elif file_name == 'curr_udpjitter':
            aggregated_df = aggregate_jitter(df, year)
        else:
            logging.error(f"Aggregation logic not found for file: {file_name}")
            continue
            
        logging.info(f"Data aggregated for file {file_name}.")

        # Now, combine year and month into a new date column.
        # Here, the first of the month is used for date.
        aggregated_df = aggregated_df.with_columns(
            pl.concat_str([
                pl.col("year").cast(pl.Utf8),
                pl.col("month").cast(pl.Utf8),
                pl.lit("01")
            ], separator="-")
            .str.strptime(pl.Datetime, format="%Y-%m-%d")
            .dt.replace_time_zone("UTC")
            .alias("date")
        )

        # Drop the intermediate year and month columns
        aggregated_df = aggregated_df.drop("year", "month")

        write_file_path = os.path.join(args.processed_folder, f"{file_name}.parquet")
        aggregated_df.collect(streaming=True).write_parquet(write_file_path)
        logging.info(f"Aggregated data written to {write_file_path}")

    logging.info("Aggregation process completed.\n-")


if __name__ == "__main__":
    main()