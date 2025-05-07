"""
Constants required for FCC MBA Raw Network Performance data ingestion pipeline and operator identification pipeline.
"""

UNIT_OPERATOR_MAP_FILE = "unit_id_mapping.csv"

FILE_LIST = [
    "curr_udplatency.parquet",
    # "curr_ping.parquet",  # Add if this file is also going to be ingested
    "curr_dlping.parquet",
    "curr_ulping.parquet",
    "curr_httpgetmt.parquet",
    "curr_httppostmt.parquet",
    "curr_udpjitter.parquet",
]

INGEST_FILE_LIST = [
    # 'curr_httpgetmt',
    # 'curr_httppostmt',
    # 'curr_ulping',
    # 'curr_dlping',
    # 'curr_udplatency'
    'curr_udpjitter'
    ]

FILTER_MAP = {
    'curr_httppostmt': ['remove_failed_tests', 'speed_high_pass_filter', 'speed_low_pass_filter', 'exclude_units'],
    'curr_httpgetmt': ['remove_failed_tests', 'speed_high_pass_filter', 'speed_low_pass_filter', 'exclude_units'],
    "curr_udplatency": ["zeros", "delta_rtt>300ms", "rtt_min<0.05ms", "less_than_50_samples", "packet_loss_10", 'exclude_units'],
    "packet_loss": ["zeros", "delta_rtt>300ms", "rtt_min<0.05ms", "less_than_50_samples", 'exclude_units'],
    "curr_ping": ["zeros", "delta_rtt>300ms", "rtt_min<0.05ms", "packet_loss_10", 'exclude_units'],
    "curr_dlping": ["zeros","rtt_min<0.05ms","less_than_50_samples", "packet_loss_10", 'exclude_units'],
    "curr_ulping": ["zeros","rtt_min<0.05ms","less_than_50_samples","packet_loss_10", 'exclude_units'],
    "curr_udpjitter": ["remove_failed_tests", "tests_without_jitter", 'exclude_units'],
}

FIXED_MAPPING = {
    "StackPath (off-net)": "sp.*-us.samknows.com|cloudflare.*-us.samknows.com|n.*.samknows.com|sk-1.uhnet.net",
    "Mlab (off-net)": "mlab",
    "Comcast (on-net)": "comcast",
    "AT&T (on-net)": "\.att\.",
    "Cox (on-net)": "cox",
    "Frontier (on-net)": "frontiernet",
    "Hawaiian Telecom (on-net)": "hawaiiantel",
    "Mediacom (on-net)": "mediacom|mchsi\.com",
    "Verizon (on-net)": "verizon",
    "Windstream (on-net)": "windstream",
    "Cincinnati Bell (on-net)": "cincinnati",
}

COMMON_2011_2015 = {
    "Level3.net (off-net)": "level3\.net",
    "CenturyLink (on-net)": "centurylink",
    "Qwest (on-net)": "qwest\.net",
    "Charter (on-net)": "charter",
    "TimeWarner (on-net)": "rr\.com",
    "Cablevision (on-net)": "cv\.net",
}

MAPPING_2016 = {
    "Level3.net (off-net)": "level3\.net",
    "CenturyLink (on-net)": "centurylink|qwest\.net",
    "Charter (on-net)": "charter",
    "TimeWarner (on-net)": "rr\.com",
    "Optimum (on-net)": "optimum|cv\.net",
}

COMMON_2017_2020 = {
    "Level3.net (off-net)": "level3\.net",
    "CenturyLink (on-net)": "centurylink|qwest\.net",
    "Charter (on-net)": "charter|rr\.com",
    "Optimum (on-net)": "optimum|cv\.net",
}

COMMON_2021_2023 = {
    "Level3.net (off-net)": "level3\.net",
    "CenturyLink (on-net)": "centurylink|qwest\.net",
    "Charter (on-net)": "charter|rr\.com",
    "Optimum (on-net)": "optimum|cv\.net",
}

OPERATOR_HOSTNAME_MAPPING = {
    "2011": FIXED_MAPPING | COMMON_2011_2015,
    "2012": FIXED_MAPPING | COMMON_2011_2015,
    "2013": FIXED_MAPPING | COMMON_2011_2015,
    "2014": FIXED_MAPPING | COMMON_2011_2015,
    "2015": FIXED_MAPPING | COMMON_2011_2015,
    "2016": FIXED_MAPPING | MAPPING_2016,
    "2017": FIXED_MAPPING | COMMON_2017_2020,
    "2018": FIXED_MAPPING | COMMON_2017_2020,
    "2019": FIXED_MAPPING | COMMON_2017_2020,
    "2020": FIXED_MAPPING | COMMON_2017_2020,
    "2021": FIXED_MAPPING | COMMON_2021_2023,
    "2022": FIXED_MAPPING | COMMON_2021_2023,
    "2023": FIXED_MAPPING | COMMON_2021_2023
}

# Used in operator_identification
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

# Used in operator_identification
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

UNIT_PROFILE_FILE = {
    "2011": "./unit_profile_files/unit-profile-march2011.csv",  # report yr'11
    "2012": "./unit_profile_files/unit-profile-combined2012.csv",  # report yr'12
    "2013": "./unit_profile_files/unit-profile-sept2013.csv",
    "2014": "./unit_profile_files/unit-profile-sept2014.csv",
    "2015": "./unit_profile_files/unit-profile-sept2015.csv",
    "2016": "./unit_profile_files/unit-profile-sept2016.csv",
    "2017": "./unit_profile_files/unit-profile-sept2017.csv",
    "2018": "./unit_profile_files/unit-profile-sept2018.csv",
    "2019": "./unit_profile_files/unit-profile-sept2019.csv",
    "2020": "./unit_profile_files/unit-profile-sept2020.csv",
    "2021": "./unit_profile_files/unit-profile-sept2021.csv",
    "2022": "./unit_profile_files/unit-profile-sept2022.xlsx",
    "2023": "./unit_profile_files/unit-profile-sept2022.xlsx",
}

EXCLUDE_UNITS_FILE = {
    "2011": "unit_profile_files/units-excluded-2011.xls",  # report yr'11
    "2012": "./unit_profile_files/units-excluded-combined2012.xlsx",  # report yr'12
    "2013": "",
    "2014": "",
    "2015": "./unit_profile_files/units-excluded-sept2015.xlsx",
    "2016": "./unit_profile_files/units-excluded-sept2016.xlsx",
    "2017": "./unit_profile_files/units-excluded-sept2017.xlsx",
    "2018": "./unit_profile_files/units-excluded-sept2018.xlsx",
    "2019": "./unit_profile_files/units-excluded-sept2019.xlsx",
    "2020": "./unit_profile_files/units-excluded-sept2020.xlsx",
    "2021": "./unit_profile_files/units-excluded-sept2021.xlsx",
    "2022": "./unit_profile_files/units-excluded-sept2022.xlsx",
    "2023": "./unit_profile_files/units-excluded-sept2022.xlsx",
}

ELASTIC_SEARCH_HOSTS = [
    "https://172.26.170.253:9200/",
    "https://172.26.170.156:9200/",
    "https://172.26.170.52:9200/",
    "https://172.26.170.20:9200/",
    "https://172.26.170.185:9200/",
    "https://172.26.170.249:9200/",
    "https://172.26.170.132:9200/",
    "https://172.26.170.208:9200/",
    "https://172.26.170.33:9200/",
    "https://172.26.170.109:9200/",
]