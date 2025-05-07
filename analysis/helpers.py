from elasticsearch import Elasticsearch, helpers
import configparser
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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


def get_client():

    config = configparser.ConfigParser()
    config.read("../elastic/security.ini")

    client = Elasticsearch(
                ELASTIC_SEARCH_HOSTS,
                ca_certs='../elastic/http_ca.crt',
                basic_auth=(config["ELASTIC"]["user"], config["ELASTIC"]["password"]),
                verify_certs=True,
                retry_on_timeout=True,
                max_retries=3,
            )
    
    if client.ping():
        print("Connected to Elasticsearch.")
        return client
    
    print("Could not connect to Elasticsearch.")
    return client


def get_data(index, es):

    # Elasticsearch query to fetch all documents from indices matching pattern
    query = {
        "query": {"match_all": {}}
    }

    # Fetch all data with scroll API
    results = helpers.scan(es, index=index, query=query)

    # Convert to pandas DataFrame
    data = [doc["_source"] for doc in results]
    return pd.DataFrame(data)