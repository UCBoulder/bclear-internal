#!/usr/bin/python3

import pandas as pd

unit_files = [
    "unit_profile_files/unit-profile-march2011.csv",  # report yr'11
    "unit_profile_files/unit-profile-april2012.csv",  # report yr'12
    "unit_profile_files/unit-profile-sept2012.csv",  # report yr'13
    "unit_profile_files/unit-profile-sept2014.csv",
    "unit_profile_files/unit-profile-sept2015.csv",
    "unit_profile_files/unit-profile-sept2016.csv",
    "unit_profile_files/unit-profile-sept2017.csv",
    "unit_profile_files/unit-profile-sept2018.csv",
    "unit_profile_files/unit-profile-sept2019.csv",
    "unit_profile_files/unit-profile-sept2020.csv",
    "unit_profile_files/unit-profile-sept2021.csv",
]

unit_isp_mapping = {}

for unit_file in unit_files:
    profile = pd.read_csv(unit_file)
    profile["combined"] = profile[["ISP", "Technology"]].values.tolist()
    temp_dict = dict(zip(profile["Unit ID"], profile["combined"]))
    unit_isp_mapping.update(temp_dict)

new_df = pd.DataFrame(
    [
        [unit_id, operator_name.strip(), operator_technology.strip()]
        for unit_id, (operator_name, operator_technology) in unit_isp_mapping.items()
    ],
    columns=["Unit ID", "ISP", "Technology"],
)
new_df.to_csv("unit-profile-combined.csv", encoding="utf-8", index=False)
