import pandas as pd
import numpy as np
import polars as pl
from constants import UNIT_PROFILE_FILE, EXCLUDE_UNITS_FILE, OPERATOR_HOSTNAME_MAPPING


def get_combined_unit_profile(year: str) -> pd.DataFrame:
    """
    Get Returns the timezone offset and DST offset.
    """

    if year == "2022" or year == "2023":
        profile = pd.read_excel(UNIT_PROFILE_FILE[year], sheet_name=0)
    else:
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
        profile["timezone_offset_dst"] = profile["timezone_offset_dst"].str.replace(" hr", "")

    if all(
            col in profile.columns.tolist()
            for col in ["timezone_offset", "timezone_offset_dst"]
    ):
        profile["timezone_offset"] = profile["timezone_offset"].map(lambda x: int(x) if str(x).isdecimal() else np.nan)
        profile["timezone_offset_dst"] = profile["timezone_offset_dst"].map(
            lambda x: int(x) if str(x).isdecimal() else np.nan)

    return profile


def create_unit_timezone_map(year: str) -> dict:
    """
    Create a mapping of unit IDs to timezone offsets.
    """
    
    unit_timezone_mapping = {}
    # read_order = ["2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012"]
    read_order = []

    for i in range(2012, int(year) + 1):
        read_order.append(str(i))

    if year == "2011":
        read_order = ["2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012"]

    for year in read_order:
        profile = get_combined_unit_profile(year)
        profile["combined"] = profile[["timezone_offset", "timezone_offset_dst"]].values.tolist()
        temp_dict = dict(zip(profile["Unit ID"], profile["combined"]))
        unit_timezone_mapping.update(temp_dict)

    return unit_timezone_mapping


def group_by_targets(data, year):
    # TODO: Use it in filter.py
    operator_expr = pl.lit("no_operator")  # default value
    for operator_name, hostname_regex in OPERATOR_HOSTNAME_MAPPING[year].items():
        operator_expr = pl.when(pl.col("target").str.contains(pattern=hostname_regex)).then(pl.lit(operator_name)).otherwise(operator_expr)
    data = data.with_columns(operator_expr.alias("operator"))
    data = data.with_columns(
        pl.when(pl.col("operator").str.contains("on-net"))
          .then(pl.lit("on-net"))
          .otherwise(pl.lit("off-net"))
          .alias("test_type")
    )
    return data
