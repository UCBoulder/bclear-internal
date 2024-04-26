#!/usr/bin/python3

CHUNK_SIZE = 5000000

FIXED_MAPPING = {
    "Comcast (on-net)": "comcast",
    "AT&T (on-net)": "\.att\.",
    "Cox (on-net)": "cox",
    "Frontier (on-net)": "frontiernet",
    "Hawaiian Telecom (on-net)": "hawaiiantel",
    "Mediacom (on-net)": "mediacom|mchsi\.com",
    "Verizon (on-net)": "verizon",
    "Windstream (on-net)": "windstream",
    "Cincinnati Bell (on-net)": "cincinnati",
    "Mlab (off-net)": "mlab",
    "StackPath (off-net)": "sp.*-us.samknows.com|cloudflare.*-us.samknows.com|n.*.samknows.com|sk-1.uhnet.net",
}

COMMON_2011_2015 = {
    "CenturyLink (on-net)": "centurylink",
    "Qwest (on-net)": "qwest\.net",
    "Charter (on-net)": "charter",
    "TimeWarner (on-net)": "rr\.com",
    "Cablevision (on-net)": "cv\.net",
    "Level3.net (off-net)": "level3\.net",
}

MAPPING_2016 = {
    "CenturyLink (on-net)": "centurylink|qwest\.net",
    "Charter (on-net)": "charter",
    "TimeWarner (on-net)": "rr\.com",
    "Optimum (on-net)": "optimum|cv\.net",
    "Level3.net (off-net)": "level3\.net",
}

COMMON_2017_2020 = {
    "CenturyLink (on-net)": "centurylink|qwest\.net",
    "Charter (on-net)": "charter|rr\.com",
    "Optimum (on-net)": "optimum|cv\.net",
    "Level3.net (off-net)": "level3\.net",
}

COMMON_2021_2023 = {
    "CenturyLink (on-net)": "centurylink|qwest\.net",
    "Charter (on-net)": "charter|rr\.com",
    "Optimum (on-net)": "optimum|cv\.net",
    "Level3.net (on-net)": "level3\.net",
}

OPERATOR_HOSTNAME_MAPPING = {
    "2011": COMMON_2011_2015 | FIXED_MAPPING,
    "2012": COMMON_2011_2015 | FIXED_MAPPING,
    "2013": COMMON_2011_2015 | FIXED_MAPPING,
    "2014": COMMON_2011_2015 | FIXED_MAPPING,
    "2015": COMMON_2011_2015 | FIXED_MAPPING,
    "2016": MAPPING_2016 | FIXED_MAPPING,
    "2017": COMMON_2017_2020 | FIXED_MAPPING,
    "2018": COMMON_2017_2020 | FIXED_MAPPING,
    "2019": COMMON_2017_2020 | FIXED_MAPPING,
    "2020": COMMON_2017_2020 | FIXED_MAPPING,
    "2021": COMMON_2021_2023 | FIXED_MAPPING,
    "2022": COMMON_2021_2023 | FIXED_MAPPING,
    "2023": COMMON_2021_2023 | FIXED_MAPPING
}

# OPERATOR_HOSTNAME_MAPPING = {
#     "Comcast (on-net)": "comcast",
#     "AT&T (on-net)": "\.att\.",
#     "CenturyLink (on-net)": "centurylink|qwest\.net", # remove qwest from for years 2011-2015
#     "Charter (on-net)": "charter",
#     "TimeWarner (on-net)": "rr\.com", # for years 2011-2016
#     "Cox (on-net)": "cox",
#     "Frontier (on-net)": "frontiernet",
#     "Hawaiian Telecom (on-net)": "hawaiiantel",
#     "Mediacom (on-net)": "mediacom|mchsi\.com",
#     "Optimum (on-net)": "optimum|cv\.net",  # uncomment from 2016-2023 onwards before that it was Cablevision
#     # "Cablevision (on-net)": "cv\.net", # comment from 2015-2023 onwards add it to Optimum, uncomment for yrs 2011-2014
#     "Verizon (on-net)": "verizon",
#     "Windstream (on-net)": "windstream",
#     "Cincinnati Bell (on-net)": "cincinnati",
#     # "Qwest (on-net)": "qwest\.net", # only for 2011-2015, after that add it to CenturyLink
#     "Mlab (off-net)": "mlab",
#     "Level3.net (off-net)": "level3\.net", # from 2011 - 2020
#     # "Level3.net (on-net)": "level3\.net", # from 2021-2023
#     "StackPath (off-net)": "sp.*-us.samknows.com|cloudflare.*-us.samknows.com|n.*.samknows.com|sk-1.uhnet.net",
# }

OPERATOR_TECHNOLOGY_MAPPING = {
    "2023": {
        "Comcast": "Cable",
        "AT&T": "Fiber",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Verizon": "Fiber",
        "Windstream": "DSL",
    },
    "2022": {
        "Comcast": "Cable",
        "AT&T": "Fiber",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Verizon": "Fiber",
        "Windstream": "DSL",
    },
    "2021": {
        "Comcast": "Cable",
        "AT&T": "Fiber",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Verizon": "Fiber",
        "Windstream": "DSL",
    },
    "2020": {
        "Comcast": "Cable",
        "AT&T": "Fiber",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Verizon": "Fiber",
        "Windstream": "DSL",
    },
    "2019": {
        "Comcast": "Cable",
        "AT&T": "UVERSE",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
    },
    "2018": {
        "Comcast": "Cable",
        "AT&T": "IPBB",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
        "CableONE": "Cable",
    },
    "2017": {
        "Comcast": "Cable",
        "AT&T": "IPBB",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "Hawaiian Telecom": "DSL",
    },
    "2016": {
        "Comcast": "Cable",
        "AT&T": "DSL",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
    },
    "2015": {
        "Comcast": "Cable",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Optimum": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
        "Qwest": "DSL",
        "Cablevision": "Cable",
    },
    "2014": {
        "Comcast": "Cable",
        "AT&T": "DSL",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
        "Qwest": "DSL",
        "Cablevision": "Cable",
    },
    "2013": {
        "Comcast": "Cable",
        "AT&T": "DSL",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
        "Qwest": "DSL",
        "Cablevision": "Cable",
    },
    "2012": {
        "Comcast": "Cable",
        "AT&T": "DSL",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
        "Qwest": "DSL",
        "Cablevision": "Cable",
        "Insight": "Cable",
    },
    "2011": {
        "Comcast": "Cable",
        "AT&T": "DSL",
        "Charter": "Cable",
        "Cox": "Cable",
        "Mediacom": "Cable",
        "Windstream": "DSL",
        "CenturyLink": "DSL",
        "Hughes": "Satellite",
        "Wildblue/ViaSat": "Satellite",
        "TimeWarner": "Cable",
        "Qwest": "DSL",
        "Cablevision": "Cable",
        "Insight": "Cable",
    },
}

OPERATOR_TECHNOLOGY_YEAR = {  # to categorize between DSL and Fiber
    "2023": [
        ("CenturyLink", 150),
        ("Hawaiian Telecom", 110),
        ("Frontier", 30),
        ("Cincinnati Bell", 110),
    ],
    "2022": [
        ("CenturyLink", 150),
        ("Hawaiian Telecom", 110),
        ("Frontier", 30),
        ("Cincinnati Bell", 110),
    ],
    "2021": [
        ("CenturyLink", 150),
        ("Hawaiian Telecom", 110),
        ("Frontier", 30),
        ("Cincinnati Bell", 110),
    ],
    "2020": [
        ("CenturyLink", 110),
        ("Hawaiian Telecom", 50),
        ("Frontier", 30),
        ("Cincinnati Bell", 60),
    ],
    "2019": [("Frontier", 30), ("Hawaiian Telecom", 50), ("Cincinnati Bell", 30), ("Verizon", 60)],
    "2018": [("Frontier", 30), ("Hawaiian Telecom", 50), ("Cincinnati Bell", 30), ("Verizon", 40)],
    "2017": [("Frontier", 15), ("Cincinnati Bell", 30), ("Verizon", 10)],
    "2016": [("Frontier", 15), ("Cincinnati Bell", 25), ("Verizon", 10)],
    "2015": [("Frontier", 15), ("Cincinnati Bell", 25), ("Verizon", 10)],
    "2014": [("Frontier", 10), ("Verizon", 10)],
    "2013": [("Frontier", 10), ("Verizon", 10)],
    "2012": [("Frontier", 10), ("Verizon", 10)],
    "2011": [("Frontier", 10), ("Verizon", 10)],
}

UNIT_OPERATOR_MAP_FILE = "unit_id_mapping.csv"

EXCLUDE_UNITS_FILE = "../units-excluded-sept2021.csv"

UNIT_PROFILE_FILE = {
    "2011": "../unit_profile_files/unit-profile-march2011.csv",  # report yr'11
    "2012": "../unit_profile_files/unit-profile-combined2012.csv",  # report yr'12
    "2013": "../unit_profile_files/unit-profile-sept2013.csv",
    "2014": "../unit_profile_files/unit-profile-sept2014.csv",
    "2015": "../unit_profile_files/unit-profile-sept2015.csv",
    "2016": "../unit_profile_files/unit-profile-sept2016.csv",
    "2017": "../unit_profile_files/unit-profile-sept2017.csv",
    "2018": "../unit_profile_files/unit-profile-sept2018.csv",
    "2019": "../unit_profile_files/unit-profile-sept2019.csv",
    "2020": "../unit_profile_files/unit-profile-sept2020.csv",
    "2021": "../unit_profile_files/unit-profile-sept2021.csv",
    "2022": "../unit_profile_files/unit-profile-sept2021.csv",
    "2023": "../unit_profile_files/unit-profile-sept2021.csv",
}

# use a dict in the filter_data.py file
EXCLUDE_UNITS_FILE = {
    "2011": "",  # report yr'11
    "2012": "../unit_profile_files/units-excluded-combined2012.xlsx",  # report yr'12
    "2013": "",
    "2014": "",
    "2015": "../unit_profile_files/units-excluded-sept2015.xlsx",
    "2016": "../unit_profile_files/units-excluded-sept2016.xlsx",
    "2017": "../unit_profile_files/units-excluded-sept2017.xlsx",
    "2018": "../unit_profile_files/units-excluded-sept2018.xlsx",
    "2019": "../unit_profile_files/units-excluded-sept2019.xlsx",
    "2020": "../unit_profile_files/units-excluded-sept2020.xlsx",
    "2021": "../unit_profile_files/units-excluded-sept2021.xlsx",
    "2022": "../unit_profile_files/units-excluded-sept2021.xlsx",
    "2023": "../unit_profile_files/units-excluded-sept2021.xlsx",
}

FILE_LIST = [
    "curr_udplatency.csv",
    "curr_ping.csv",
    "curr_dlping.csv",
    "curr_ulping.csv",
    "curr_httpgetmt.csv",
    "curr_httppostmt.csv",
    "curr_udpjitter.csv",
]

UPLOAD_FILE_LIST = [
    # "curr_udplatency.csv",
    # "curr_ulping.csv",
    # "curr_dlping.csv",
    # "curr_ping.csv",
    # "curr_httpgetmt.csv",
    # "curr_httppostmt.csv",
    "curr_udpjitter.csv",
]  

LATENCY_FILE_LIST = [
    "curr_udplatency.csv",
    "curr_ulping.csv",
    "curr_dlping.csv",
    "curr_ping.csv",
    "curr_udpjitter.csv",
]

FILE_FILTER_MAP = {
    "curr_udplatency.csv": [
        "zeros",
        "delta_rtt>300ms",
        "rtt_min<0.05ms",
        "less_than_50_samples",
        "packet_loss_10",
    ],
    "curr_ping.csv": ["zeros", "delta_rtt>300ms", "rtt_min<0.05ms", "packet_loss_10"],
    "curr_dlping.csv": [
        "zeros",
        "rtt_min<0.05ms",
        "less_than_50_samples", 
        "packet_loss_10",
    ],
    "curr_ulping.csv": [
        "zeros",
        "rtt_min<0.05ms",
        "less_than_50_samples",
        "packet_loss_10",
    ],
    "curr_httpgetmt.csv": ["failed_tests", "failed_speed_tests"],
    "curr_httppostmt.csv": ["failed_tests", "failed_speed_tests"],
    "curr_udpjitter.csv": ["failed_tests", "tests_without_jitter"],
}

VALIDATED_DATA_FOLDER = "validated_data"
