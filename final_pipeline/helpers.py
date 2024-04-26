#!/usr/bin/python3
import gc

import pandas as pd
import numpy as np

from constants import OPERATOR_HOSTNAME_MAPPING, UNIT_PROFILE_FILE, EXCLUDE_UNITS_FILE


class OperatorInfo(object):
    name = ""
    df = ""

    def __init__(self, name) -> None:
        self.name = name
        pass


def group_by_targets(data, year):
    operators_info = []
    rem_data = data

    for operator_name, hostname_regex in OPERATOR_HOSTNAME_MAPPING[year].items():
        operatorInfo = OperatorInfo(operator_name)
        operatorInfo.df = rem_data[
            rem_data["target"].str.contains(hostname_regex, na=False)
        ]
        rem_data = rem_data[~rem_data["target"].str.contains(hostname_regex, na=False)]
        operators_info.append(operatorInfo)

    rem_targets = rem_data["target"].unique().tolist()
    del [rem_data]
    gc.collect()

    return operators_info, rem_targets


def get_target_stats(data, year, tabulate=False):
    target_results = []
    operators_info, _ = group_by_targets(data, year)

    for info in operators_info:
        target_results.append(
            [info.name, info.df["target"].unique().shape[0], info.df.shape[0]]
        )
        # print(f"\n\n Records in {info.name} are: {info.df['target'].unique()}")

    df = pd.DataFrame(
        target_results,
        columns=["ISP Name", "Number of servers", "Total number of entires in table"],
    )

    # df = df.append(pd.DataFrame([df["Number of servers"].sum(), df["Total number of entires in table"].sum()],
    #                             index = ["Total"], columns=['Number of servers', 'Total number of entires in table']))

    if tabulate:
        df.loc["Total"] = df[
            ["Number of servers", "Total number of entires in table"]
        ].sum()
        df = df.sort_values("Total number of entires in table")
        df = df.fillna("")

        print("\nThe summary of targets is: ")
        print(df.to_markdown(tablefmt="grid", floatfmt=""))

        # rem = rem_data['target'].unique()
        # print(f"\n\nA total of {rem.shape[0]} records are not classified they are: \n{rem}")

    return df


def is_int(string):
    try:
        _ = int(string)
        return True
    except ValueError:
        return False
    

def get_combined_unit_profile(year):

    profile = pd.read_csv(UNIT_PROFILE_FILE[year], on_bad_lines="skip")

    if EXCLUDE_UNITS_FILE.get(year, False):
        excluded_df = pd.read_excel(EXCLUDE_UNITS_FILE.get(year), sheet_name=0)
        excluded_df.rename(
            columns={excluded_df.columns[1]: "ISP", excluded_df.columns[0]: "Unit ID"},
            inplace=True,
        )

        profile = (
            pd.concat([profile, excluded_df])
            .drop_duplicates(subset=["Unit ID"])
            .reset_index(drop=True)
        )
        profile["Technology"] = profile["Technology"].fillna("")
    
    if year == "2017":
        profile["timezone_offset"] = profile["timezone_offset"].str.replace(" hr", "")
        profile["timezone_offset_dst"] = profile["timezone_offset_dst"].str.replace(
            " hr", ""
        )

    if all(
        col in profile.columns.tolist()
        for col in ["timezone_offset", "timezone_offset_dst"]
    ):
        profile["timezone_offset"] = profile["timezone_offset"].map(lambda x: int(x) if is_int(x) else np.nan)
        profile["timezone_offset_dst"] = profile["timezone_offset_dst"].map(lambda x: int(x) if is_int(x) else np.nan)
    
    return profile

def create_unit_timezone_map(yr):
    unit_timezone_mapping = {}
    # read_order = ["2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012"]
    read_order = []
    
    for i in range(2012, int(yr)+1):
        read_order.append(str(i))
    
    if yr == "2011":
        read_order = ["2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012"]
    
    for yr in read_order:
        profile = get_combined_unit_profile(yr)
        profile["combined"] = profile[["timezone_offset", "timezone_offset_dst"]].values.tolist()
        temp_dict = dict(zip(profile["Unit ID"], profile["combined"]))
        unit_timezone_mapping.update(temp_dict)
    
    return unit_timezone_mapping