#!/usr/bin/python3
import argparse
import gc
import logging
import os
from collections import defaultdict

import pandas as pd
import numpy as np

from constants import (
    CHUNK_SIZE,
    EXCLUDE_UNITS_FILE,
    FILE_LIST,
    OPERATOR_TECHNOLOGY_MAPPING,
    OPERATOR_TECHNOLOGY_YEAR,
    UNIT_OPERATOR_MAP_FILE,
    UNIT_PROFILE_FILE,
)
from helpers import group_by_targets, create_unit_timezone_map

logging.basicConfig(
    filename="get_transform_load_new.log",
    filemode="a",
    format="%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


def get_all_unit_ids(file_path_list):
    all_unit_ids = set()
    for file_path in file_path_list:
        if os.path.isfile(file_path):
            data_chunks = pd.read_csv(
                file_path, on_bad_lines="skip", chunksize=CHUNK_SIZE
            )
            for data in data_chunks:
                all_unit_ids.update(data.unit_id.unique().tolist())
    return all_unit_ids


def get_operator_name(file_path_list, unit_id_operator_mapping, year):
    for file_path in file_path_list:
        rem_targets = set()
        if not os.path.isfile(file_path):
            continue
        data_chunks = pd.read_csv(file_path, on_bad_lines="skip", chunksize=CHUNK_SIZE)
        for data in data_chunks:
            operator_grouping, targets = group_by_targets(data, year)
            rem_targets.update(targets)

            for operator_info in operator_grouping:
                if (
                    "off-net" in operator_info.name
                ):  # ignore Stackpath, level3.net, mlab and only consider on-net servers
                    continue
                unit_ids = operator_info.df.unit_id.unique().tolist()
                unit_id_operator_mapping.update(
                    dict.fromkeys(
                        unit_ids,
                        [
                            operator_info.name.split("(")[0].strip(),
                            "",
                            0,
                            0,
                            "bmc",
                            0,
                            0,
                        ],
                    )
                )

        logging.info(
            f"The remaining targets for file: {file_path} are: {list(rem_targets)}"
        )

    return unit_id_operator_mapping


def get_unit_profile_filepath(root_folder):
    year = root_folder.split("/")[-1][:4]  # [:4] to keep just the year
    return UNIT_PROFILE_FILE[year]


def getAvg(val):
    val = val[1:-1]  # removes the chars '[', ']' at start and end
    l = float(val.split("-")[0])
    h = float(val.split("-")[1]) if len(val.split("-")) == 2 else 0
    return (l + h) / 2 if h != 0 else l


def is_float(string):
    try:
        _ = float(string)
        return True
    except ValueError:
        return False


def is_int(string):
    try:
        _ = int(string)
        return True
    except ValueError:
        return False

def get_timezone_offset(unit_timezone_map, unit_id):
    if unit_id in unit_timezone_map:
        return unit_timezone_map[unit_id]
    return [np.nan, np.nan]

def get_combined_unit_profile(year):
    # year = root_folder.split("/")[-1][:4]  # [:4] to keep just the year
    logging.info(f"The year is {year}")

    profile = pd.read_csv(UNIT_PROFILE_FILE[year], on_bad_lines="skip")
    profile["validation_type"] = "fcc"

    if EXCLUDE_UNITS_FILE.get(year, False):
        excluded_df = pd.read_excel(EXCLUDE_UNITS_FILE.get(year), sheet_name=0)
        excluded_df.rename(
            columns={excluded_df.columns[1]: "ISP", excluded_df.columns[0]: "Unit ID"},
            inplace=True,
        )
        if year == "2012":
            excluded_df["Download"] = excluded_df["Download"].str.replace("Mbit/s", "")
            excluded_df["Upload"] = excluded_df["Upload"].str.replace("Mbit/s", "")

        profile = (
            pd.concat([profile, excluded_df])
            .drop_duplicates(subset=["Unit ID"])
            .reset_index(drop=True)
        )
        profile["Technology"] = profile["Technology"].fillna("")

    if all(col in profile.columns.tolist() for col in ["Download", "Upload"]):
        profile["Download"] = profile["Download"].map(
            lambda x: getAvg(x) if (isinstance(x, str) and "[" in x) else x
        )
        profile["Upload"] = profile["Upload"].map(
            lambda x: getAvg(x) if (isinstance(x, str) and "[" in x) else x
        )
        profile["Download"] = profile["Download"].map(
            lambda x: float(x) if is_float(x) else 0
        )
        profile["Upload"] = profile["Upload"].map(
            lambda x: float(x) if is_float(x) else 0
        )

    if year == "2017":
        profile["timezone_offset"] = profile["timezone_offset"].str.replace(" hr", "")
        profile["timezone_offset_dst"] = profile["timezone_offset_dst"].str.replace(
            " hr", ""
        )
    elif year == "2011":
        unit_timezone_map = create_unit_timezone_map(year)
        profile[['timezone_offset','timezone_offset_dst']] = profile.apply(
            lambda row: pd.Series(get_timezone_offset(unit_timezone_map, row["Unit ID"])),
            axis=1
        )

    if all(
        col in profile.columns.tolist()
        for col in ["timezone_offset", "timezone_offset_dst"]
    ):
        profile["timezone_offset"] = profile["timezone_offset"].map(lambda x: int(x) if is_int(x) else np.nan)
        profile["timezone_offset_dst"] = profile["timezone_offset_dst"].map(lambda x: int(x) if is_int(x) else np.nan)

    # for 2020 and 2018
    profile["ISP"] = profile["ISP"].str.strip()
    profile["ISP"] = profile["ISP"].replace("Verizon DSL", "Verizon")
    profile["ISP"] = profile["ISP"].replace("Verizon Fiber", "Verizon")
    profile["ISP"] = profile["ISP"].replace("Verizon Wireless", "Verizon")
    profile["ISP"] = profile["ISP"].replace("Verizon Business", "Verizon")
    profile["ISP"] = profile["ISP"].replace("Hawaiian Telcom", "Hawaiian Telecom")
    # for 2016
    profile["ISP"] = profile["ISP"].replace("AT&T DSL", "AT&T")
    profile["ISP"] = profile["ISP"].replace("AT&T IPBB", "AT&T")
    profile["ISP"] = profile["ISP"].replace("Cincinnati Bell DSL", "Cincinnati Bell")
    profile["ISP"] = profile["ISP"].replace("Cincinnati Bell Fiber", "Cincinnati Bell")
    profile["ISP"] = profile["ISP"].replace("Frontier DSL", "Frontier")
    profile["ISP"] = profile["ISP"].replace("Frontier Fiber", "Frontier")
    profile["ISP"] = profile["ISP"].replace("TWC", "TimeWarner")
    profile["ISP"] = profile["ISP"].replace("Time Warner Cable", "TimeWarner")
    profile["ISP"] = profile["ISP"].replace("Oceanic TWC", "TimeWarner")

    profile["Technology"] = profile["Technology"].str.strip()
    profile["Technology"] = profile["Technology"].replace("CABLE", "Cable")
    profile["Technology"] = profile["Technology"].replace("CABLE - BUSINESS", "Cable")
    profile["Technology"] = profile["Technology"].replace("FIBER", "Fiber")
    profile["Technology"] = profile["Technology"].replace("SATELLITE", "Satellite")
    profile["Technology"] = profile["Technology"].replace("SAT", "Satellite")
    profile["Technology"] = profile["Technology"].replace("WIRELESS", "Wireless")

    # make misc and remove entries as none
    profile["ISP"] = profile["ISP"].replace("Miscellaneous", "")
    profile["Technology"] = profile["Technology"].replace("REMOVE", "")
    profile["Technology"] = profile["Technology"].replace("MISC", "")
    profile["Technology"] = profile["Technology"].replace("Unknown", "")

    profile["Download"] = profile["Download"].fillna(0)
    profile["Upload"] = profile["Upload"].fillna(0)
    profile["validation_type"] = profile["validation_type"].fillna("bmc")

    return profile


def get_unit_operator_map(root_folder):
    unit_id_operator_mapping = {}
    year = root_folder.split("/")[-1][:4]  # [:4] to keep just the year
    file_path_list = [os.path.join(root_folder, file) for file in FILE_LIST]

    all_unit_ids = get_all_unit_ids(file_path_list)

    logging.debug(
        f"Starting operator_name classification process for {root_folder.split('/')[-1]}"
    )

    unit_id_operator_mapping = get_operator_name(
        file_path_list, unit_id_operator_mapping, year
    )

    logging.debug(f"total count of all ids: {len(all_unit_ids)}")
    classified_unit_ids = set(unit_id_operator_mapping.keys())
    logging.debug(f"count of classified ids: {len(classified_unit_ids)}")
    unclassified_unit_ids = all_unit_ids - classified_unit_ids
    logging.debug(f"count of unclassified ids: {len(unclassified_unit_ids)}")

    logging.debug(f"Using unit_profile file to further classify the remaining units")

    # validated_units = pd.read_csv(get_unit_profile_filepath(root_folder), on_bad_lines='skip')
    # validated_unit_ids = validated_units.unit_ids.tolist()
    unit_profile = get_combined_unit_profile(year)

    new_df = unit_profile[unit_profile["Unit ID"].isin(unclassified_unit_ids)]

    ## testing code
    profile_units = set(unit_profile["Unit ID"].unique().tolist())
    not_present = classified_unit_ids - profile_units
    logging.debug(
        f"count of unit_ids classified based on target server but not present in unit-profile file: {len(not_present)}"
    )
    logging.debug(f"5 of the units are: {list(not_present)[:5]}")
    ## ends

    if "Download" not in new_df.columns.tolist():
        new_df["Download"] = 0
        unit_profile["Download"] = 0
    if "Upload" not in new_df.columns.tolist():
        new_df["Upload"] = 0
        unit_profile["Upload"] = 0
    if "timezone_offset" not in new_df.columns.tolist():
        new_df["timezone_offset"] = 0
        unit_profile["timezone_offset"] = 0
    if "timezone_offset_dst" not in new_df.columns.tolist():
        new_df["timezone_offset_dst"] = 0
        unit_profile["timezone_offset_dst"] = 0
    for _, row in new_df.iterrows():
        unit_id_operator_mapping[row["Unit ID"]] = [
            row["ISP"],
            "",
            row["Download"],
            row["Upload"],
            row["validation_type"],
            row["timezone_offset"],
            row["timezone_offset_dst"],
        ]

    # for storing the download and upload speeds for IDs classified using group_by_targets function.
    # speeds are gotten from the unit profile file
    for _, row in unit_profile.iterrows():
        if row["Unit ID"] in unit_id_operator_mapping:
            isp = unit_id_operator_mapping[row["Unit ID"]][0]
            tech = unit_id_operator_mapping[row["Unit ID"]][1]
            unit_id_operator_mapping[row["Unit ID"]] = [
                isp,
                tech,
                row["Download"],
                row["Upload"],
                row["validation_type"],
                row["timezone_offset"],
                row["timezone_offset_dst"],
            ]

    logging.debug(f"total count of all ids: {len(all_unit_ids)}")
    classified_unit_ids = set(unit_id_operator_mapping.keys())
    logging.debug(f"count of classified ids: {len(classified_unit_ids)}")
    unclassified_unit_ids = all_unit_ids - classified_unit_ids
    unclassified_unit_ids_count = len(unclassified_unit_ids)
    for unit_id, (op_name, op_tech, _, _, _, _, _) in unit_id_operator_mapping.items():
        if op_name == "":
            unclassified_unit_ids_count += 1
    logging.debug(f"Final count of unclassified ids: {unclassified_unit_ids_count}")

    logging.debug(f"Next, operator_technology classification")
    # assign technology for all the operators that use only one technology
    op_map = OPERATOR_TECHNOLOGY_MAPPING[year]
    for unit_id, (
        op_name,
        op_tech,
        download_speed,
        upload_speed,
        validation_type,
        timezone_offset,
        timezone_offset_dst,
    ) in unit_id_operator_mapping.items():
        if op_name in op_map:
            unit_id_operator_mapping[unit_id] = [
                op_name,
                op_map[op_name],
                download_speed,
                upload_speed,
                validation_type,
                timezone_offset,
                timezone_offset_dst,
            ]

    operator_unit_id_mapping = defaultdict(list)
    for unit_id, (op_name, op_tech, _, _, _, _, _) in unit_id_operator_mapping.items():
        operator_unit_id_mapping[op_name].append(unit_id)

    unclassified_tech_count = 0

    logging.debug(f"Using unit_profile file to classify the remaining units")
    for _, row in unit_profile.iterrows():
        unit_id = row["Unit ID"]
        if unit_id in unit_id_operator_mapping:
            unit_info = unit_id_operator_mapping[unit_id]
            isp = unit_info[0]
            if row["Technology"] != "" and row["ISP"] == isp:
                # if unit_id_operator_mapping[unit_id][1] == "": # technology is not present
                unit_id_operator_mapping[unit_id] = [
                    isp,
                    row["Technology"],
                    unit_info[2],
                    unit_info[3],
                    unit_info[4],
                    unit_info[5],
                    unit_info[6],
                ]

    for unit_id, (op_name, op_tech, _, _, _, _, _) in unit_id_operator_mapping.items():
        if op_tech == "":
            unclassified_tech_count += 1
    logging.debug(f"count of unclassified technology: {unclassified_tech_count}")

    logging.debug(
        f"Using curr_httpgetmt to identify technology for ISP's with multiple technology type"
    )
    download_file = os.path.join(root_folder, "curr_httpgetmt.csv")
    # data = pd.read_csv(download_file, on_bad_lines='skip')
    data_chunks = pd.read_csv(download_file, on_bad_lines="skip", chunksize=CHUNK_SIZE)
    for data in data_chunks:
        for operator, cutoff_speed in OPERATOR_TECHNOLOGY_YEAR[year]:
            # for operator, cutoff_speed in [('CenturyLink', 150), ('Hawaiian Telecom', 110), ('Frontier', 30), ('Cincinnati Bell', 110)]:
            unit_ids = operator_unit_id_mapping[operator]
            # logging.debug(f"count in map {len(unit_ids)}")
            operator_speeds = data[data["unit_id"].isin(unit_ids)]
            # logging.debug(f"count from table is {len(operator_speeds.unit_id.unique())}")

            download_info = operator_speeds.groupby(["unit_id"], as_index=False)[
                "bytes_sec"
            ].mean(numeric_only=True)
            download_info["bytes_sec"] = (
                download_info["bytes_sec"].mul(8).div(1024 * 1024).round(2)
            )
            for _, row in download_info.iterrows():
                unit_id = row["unit_id"]
                if unit_id_operator_mapping[unit_id][1] == "":
                    download_speed = unit_id_operator_mapping[unit_id][2]
                    upload_speed = unit_id_operator_mapping[unit_id][3]
                    validation_type = unit_id_operator_mapping[unit_id][4]
                    timezone_offset = unit_id_operator_mapping[unit_id][5]
                    timezone_offset_dst = unit_id_operator_mapping[unit_id][6]
                    if row["bytes_sec"] <= cutoff_speed:
                        unit_id_operator_mapping[unit_id] = [
                            operator,
                            "DSL",
                            download_speed,
                            upload_speed,
                            validation_type,
                            timezone_offset,
                            timezone_offset_dst,
                        ]
                    else:
                        unit_id_operator_mapping[unit_id] = [
                            operator,
                            "Fiber",
                            download_speed,
                            upload_speed,
                            validation_type,
                            timezone_offset,
                            timezone_offset_dst,
                        ]

    unclassified_tech_units = []
    unclassified_tech_count = 0
    for unit_id, (op_name, op_tech, _, _, _, _, _) in unit_id_operator_mapping.items():
        if op_tech == "":
            unclassified_tech_count += 1
            unclassified_tech_units.append(unit_id)
    logging.debug(f"Final count of unclassified technology: {unclassified_tech_count}")
    logging.debug(
        f"some of the unclassified technology unit IDs are: {unclassified_tech_units[:10]}"
    )

    # write to file
    unit_id_map = pd.DataFrame(
        [
            [
                unit_id,
                operator_name,
                operator_technology,
                download_speed,
                upload_speed,
                validation_type,
                timezone_offset,
                timezone_offset_dst,
            ]
            for unit_id, (
                operator_name,
                operator_technology,
                download_speed,
                upload_speed,
                validation_type,
                timezone_offset,
                timezone_offset_dst,
            ) in unit_id_operator_mapping.items()
        ],
        columns=[
            "unit_id",
            "operator_name",
            "operator_technology",
            "download_speed",
            "upload_speed",
            "validation_type",
            "timezone_offset",
            "timezone_offset_dst",
        ],
    )
    file_path = os.path.join(root_folder, UNIT_OPERATOR_MAP_FILE)
    logging.debug(f"writing to {file_path} size is {unit_id_map.shape}")
    unit_id_map.to_csv(file_path, encoding="utf-8", index=False)

    # log the agg info of unit_ids for this month
    agg_info = unit_id_map.groupby(["operator_name", "operator_technology"]).agg(
        {"unit_id": "count"}
    )
    agg_info.loc["Total"] = agg_info[["unit_id"]].sum()
    logging.info(
        f"The ISP stats for current month is: \n {agg_info.to_markdown(tablefmt='grid')}"
    )

    return [
        len(all_unit_ids),
        len(classified_unit_ids),
        len(classified_unit_ids) - unclassified_tech_count,
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-folder", type=str, help="data-folder path ")
    args = parser.parse_args()

    logging.info(
        f"generating unit_id to operator mapping file for the month of {args.data_folder}"
    )
    overall_count = []
    overall_count.append(get_unit_operator_map(args.data_folder))


if __name__ == "__main__":
    main()
