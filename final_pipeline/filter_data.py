#!/usr/bin/python3
import argparse
import gc
import logging
import os
from datetime import datetime, time, timedelta
from helpers import create_unit_timezone_map

import pandas as pd
import numpy as np
import pytz

from constants import (
    CHUNK_SIZE,
    EXCLUDE_UNITS_FILE,
    FILE_FILTER_MAP,
    UNIT_OPERATOR_MAP_FILE,
    UPLOAD_FILE_LIST,
    VALIDATED_DATA_FOLDER,
    LATENCY_FILE_LIST,
)
from helpers import group_by_targets

logging.basicConfig(
    filename="get_transform_load_new.log",
    filemode="a",
    format="%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


def get_cumulative_filter_counts(
    year, file_name, curr_data, filter_list=["zeros"], ignore_list=["failures"]
):
    filtering_results = []
    initial_count = curr_data.shape[0]
    str_col = ["target", "dtime"]  # columns that are in string format
    column_names = list(curr_data.columns.values)
    rem_data = curr_data
    match_data = curr_data[0:0]

    if year == "2011" and file_name in ["curr_ulping.csv", "curr_dlping.csv"] and 'less_than_50_samples' in filter_list:
        filter_list.remove('less_than_50_samples')

    if "zeros" in filter_list:
        for col in column_names:
            if col not in (str_col + ignore_list):
                filtered_indices = rem_data[col] == 0
                match_data = rem_data[filtered_indices]
                filter_count = filtered_indices.sum()
                filtering_results.append(
                    [
                        f"{col}_zeros (Filter 1)",
                        filter_count,
                        round((filter_count / initial_count) * 100, 2),
                    ]
                )
                rem_data = rem_data[~filtered_indices]

    if "delta_rtt>300ms" in filter_list:
        filtered_indices = (rem_data.rtt_max - rem_data.rtt_min) > 300000
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "delta_rtt>300ms (Filter 2)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    if "rtt_min<0.05ms" in filter_list:
        filtered_indices = rem_data.rtt_min < 50
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "rtt_min<0.05ms (Filter 3)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    if "less_than_50_samples" in filter_list:
        filtered_indices = rem_data.successes + rem_data.failures < 50
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "less_than_50_samples (Filter 4)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    if "packet_loss_10" in filter_list:
        filtered_indices = (
            rem_data.failures / (rem_data.failures + rem_data.successes) > 0.1
        )
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "packet_loss_10 (Filter 5)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    if "failed_tests" in filter_list:  # for httpgetmt, httppostmt, and udpjitter
        filtered_indices = rem_data.failures == 1
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "Failed tests (Filter 2)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    if (
        "failed_speed_tests" in filter_list
        and "bytes_sec_interval" in rem_data.columns.tolist()
    ):  # for httpgetmt and httppostmt
        filtered_indices = (rem_data.bytes_sec <= 0) & (
            rem_data.bytes_sec_interval <= 0
        )
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "Failed speed tests (Filter 3)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]
    
    if "tests_without_jitter" in filter_list: # for udpjitter
        filtered_indices = (rem_data.jitter_up <= 0) | (
            rem_data.jitter_down <= 0
        )
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "Tests without jitter (Filter 3)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    if "exclude_units" in filter_list:
        exclude_df = pd.read_csv(EXCLUDE_UNITS_FILE)
        exclude_list = exclude_df["Unit ID"].unique()
        filtered_indices = rem_data.unit_id.isin(exclude_list)
        match_data = rem_data[filtered_indices]
        filter_count = filtered_indices.sum()
        filtering_results.append(
            [
                "excluded unit_ids (Filter 6)",
                filter_count,
                round((filter_count / initial_count) * 100, 2),
            ]
        )
        rem_data = rem_data[~filtered_indices]

    results = pd.DataFrame(
        filtering_results,
        columns=["Filter applied", "Filter Count", "Percentage of initial data"],
    )
    results.loc["Total"] = results[["Filter Count", "Percentage of initial data"]].sum()
    results = results.fillna("")
    return results, rem_data, match_data


def merge_files(root_folder, raw_data):
    unit_map_file = os.path.join(root_folder, UNIT_OPERATOR_MAP_FILE)
    unit_mapping = pd.read_csv(unit_map_file)
    merged_df = pd.merge(raw_data, unit_mapping, how="left", on="unit_id")
    return merged_df


def write_data_to_csv(data, root_folder, file_name):
    data.drop(columns=["timezone_offset", "timezone_offset_dst"], inplace=True, errors="ignore")
    if file_name in LATENCY_FILE_LIST:
        data.drop(columns=["download_speed", "upload_speed"], inplace=True, errors="ignore")
    validated_data_folder_path = os.path.join(root_folder, VALIDATED_DATA_FOLDER)
    if not os.path.exists(validated_data_folder_path):
        os.mkdir(validated_data_folder_path)
    write_file_path = os.path.join(root_folder, VALIDATED_DATA_FOLDER, file_name)
    data.to_csv(
        write_file_path,
        encoding="utf-8",
        index=False,
        mode="a",
        header=not os.path.exists(write_file_path),
    )


def classify_on_off_net(data, year):
    pd.options.mode.chained_assignment = None
    data.insert(11, "test_type", "")
    operator_grouping, _ = group_by_targets(data, year)
    new_df = data[0:0]
    for operator_info in operator_grouping:
        if "on-net" in operator_info.name:
            operator_info.df.loc[:, "test_type"] = "on-net"
        else:
            operator_info.df.loc[:, "test_type"] = "off-net"
        new_df = pd.concat([new_df, operator_info.df])
    return new_df


def is_dst(dt):
    if dt.year < 2007:
        raise ValueError()
    dst_start = datetime(dt.year, 3, 8, 2, 0) # second sunday in march
    dst_start += timedelta(6 - dst_start.weekday())
    dst_end = datetime(dt.year, 11, 1, 2, 0) # first sunday in november
    dst_end += timedelta(6 - dst_end.weekday())
    return dst_start <= dt < dst_end


def localize_date(dtime, offset, offset_dst):
    if not offset or pd.isna(offset):
        return dtime
    
    dtObj = datetime.strptime(dtime, "%Y-%m-%d %H:%M:%S")
    if is_dst(dtObj):
        dtObj += timedelta(hours=int(offset_dst))
    else:
        dtObj += timedelta(hours=int(offset))
    
    return dtObj.strftime("%Y-%m-%d %H:%M:%S")


def get_time_category(dtime, offset, offset_dst):
    if not offset or pd.isna(offset):
        return ""

    local_time = localize_date(dtime, offset, offset_dst)
    ltObj = datetime.strptime(local_time, "%Y-%m-%d %H:%M:%S")
    

    if ltObj.weekday() >=0 and ltObj.weekday() <=4:
        seven_pm = time(19, 0, 0)
        eleven_pm = time(22, 59, 59)
        if ltObj.time() >= seven_pm and ltObj.time() <= eleven_pm:
            return "peak-hours"
        else:
            return "off-peak-hours"
    else:
        return "sat-sun"
    

def add_timezone(unit_timezone_map, unit_id, dtime, callback):
    if unit_id in unit_timezone_map:
        offset, offset_dst = unit_timezone_map[unit_id]
        return callback(dtime, offset, offset_dst)
    else:
        return callback(dtime, np.nan, np.nan)


# def localize_date(dtime, offset, offset_dst):
    # pst = pytz.timezone("UTC")
    # if offset == -5:
    #     pst = pytz.timezone("US/Eastern")
    # elif offset == -6:
    #     pst = pytz.timezone("US/Central")
    # elif offset == -7:
    #     pst = pytz.timezone("US/Mountain")
    # elif offset == -8:
    #     pst = pytz.timezone("US/Pacific")
    # elif offset == -9:
    #     pst = pytz.timezone("US/Alaska")
    # elif offset == -10:
    #     pst = pytz.timezone("US/Hawaii")

    # dtObj = datetime.strptime(dtime, "%Y-%m-%d %H:%M:%S")
    # dtObj += timedelta(hours=int(offset))
    # dtcOffset = pst.localize(dtObj).dst()
    # dtObj += dtcOffset

    # return dtObj.strftime("%Y-%m-%d %H:%M:%S")


def update_date(data, year):
    unit_timezone_map = create_unit_timezone_map(year)

    logging.debug(f"Adding field to categorize into one of Peak/Off-peak hours and sat-sun")
    data["time_category"] = data.apply(
        lambda row: get_time_category(row["dtime"], row["timezone_offset"], row["timezone_offset_dst"])
        if not pd.isna(row["timezone_offset"])
        else add_timezone(unit_timezone_map, row["unit_id"], row["dtime"], get_time_category),
        axis=1,
        )
    
    logging.debug(f"Adding field to store local time after applying timezone offset")
    data["dtime_local"] = data.apply(
        lambda row: localize_date(row["dtime"], row["timezone_offset"], row["timezone_offset_dst"])
        if not pd.isna(row["timezone_offset"])
        else add_timezone(unit_timezone_map, row["unit_id"], row["dtime"], localize_date),
        axis=1,
    )
    
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-folder", type=str, help="data-folder path ")
    args = parser.parse_args()

    file_path_list = [os.path.join(args.data_folder, file) for file in UPLOAD_FILE_LIST]
    year = args.data_folder.split("/")[-1][:4]

    for file_path in file_path_list:
        chunk = 0
        file_name = file_path.split("/")[-1]
        if os.path.isfile(file_path):
            logging.info(f"Validating the file {file_path}")
            # data = pd.read_csv(file_path)
            data_chunks = pd.read_csv(
                file_path, on_bad_lines="skip", chunksize=CHUNK_SIZE
            )
            for data in data_chunks:
                chunk += 1
                logging.debug(f"Working on the chunk {chunk} for file {file_name}")
                filter_list = FILE_FILTER_MAP[
                    file_name
                ]  # @TODO Also include files other than latency files
                results, filtered_data, _ = get_cumulative_filter_counts(
                    year, file_name, data, filter_list=filter_list
                )
                logging.debug("\n")
                logging.debug(results.to_markdown(tablefmt="grid", floatfmt=".2f"))
                del [data]
                del [results]
                gc.collect()

                logging.debug(f"Merging file {file_name} with unit_id map")
                merged_df = merge_files(args.data_folder, filtered_data)
                del [filtered_data]
                gc.collect()

                updated_df = update_date(merged_df, year)
                del [merged_df]
                gc.collect()

                logging.debug(f"Classifying each test as either on-net or off-net")
                new_df = classify_on_off_net(updated_df, year)
                del [updated_df]
                gc.collect()

                logging.debug(f"Write validated data of {file_name} to csv.")
                write_data_to_csv(new_df, args.data_folder, file_name)
                del [new_df]
                gc.collect()
        else:
            logging.info(
                f"File {file_name} is not present for yr {args.data_folder.split('/')[-1][:4]}"
            )

    logging.info("Done validating all the files")


if __name__ == "__main__":
    main()

