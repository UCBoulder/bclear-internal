import argparse
import configparser
import elasticsearch
from elasticsearch import Elasticsearch, helpers
import logging
import os
import pyarrow.parquet as pq

from constants import INGEST_FILE_LIST, ELASTIC_SEARCH_HOSTS


logging.basicConfig(
    filename="./logging/ingest.log",
    filemode="a",
    format="%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


def get_client(config):
    
    client = Elasticsearch(
                ELASTIC_SEARCH_HOSTS,
                ca_certs=config["ELASTIC"]["ca_cert"],
                basic_auth=(config["ELASTIC"]["user"], config["ELASTIC"]["password"]),
                verify_certs=True,
                retry_on_timeout=True,
                max_retries=3,
            )
    
    if client.ping():
        logging.info("Connected to Elasticsearch.")
        return client, True
    
    logging.info("Could not connect to Elasticsearch.")
    return client, False


def delete_existing_index(es, index_name):
    """
    Delete an existing Elasticsearch index if it exists.
    """
    try:
        res = es.indices.delete(index=index_name)
        logging.info(f"Existing index {index_name} deleted for overwriting.")
    except elasticsearch.NotFoundError:
        logging.info("Index does not exist. New one will be created.")


def generate_actions(parquet_path, index_name, batch_size=1000):
    """
    Generator function that reads a Parquet file in row groups and yields actions
    for Elasticsearch's bulk API.
    """
    # Open the Parquet file
    parquet_file = pq.ParquetFile(parquet_path)
    
    # Process each row group separately
    for rg in range(parquet_file.num_row_groups):
        # Read a single row group into a table
        table = parquet_file.read_row_group(rg)
        # Convert the table into a dictionary (each key is a column)
        table_dict = table.to_pydict()
        # Determine number of rows in the row group
        num_rows = len(next(iter(table_dict.values())))
        
        # Process the row group in smaller batches
        for start in range(0, num_rows, batch_size):
            end = min(start + batch_size, num_rows)
            for row in range(start, end):
                # Build a record by extracting the row-th element from each column
                record = {col: table_dict[col][row] for col in table_dict}
                yield {
                    "_index": index_name,
                    "_source": record,
                }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-folder", type=str, help="Path to folder containing filtered data")
    parser.add_argument("--month", type=str, help="Month and Year in format YYYYMM")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read("elastic/security.ini")

    # Path to your huge Parquet file
    file_path_list = [os.path.join(args.processed_folder, file+'.parquet') for file in INGEST_FILE_LIST]

    # Connect to Elasticsearch
    es, status = get_client(config)
    if not status:
        logging.info("Elastic Client could not be loaded.")
        return

    for file_path in file_path_list:

        if not os.path.isfile(file_path):
            logging.info(f"File {file_path} is not present.")
            continue
        
        file_name = file_path.split("/")[-1][:-8]
        if file_name == 'curr_udpjitter' and args.month in ['201102', '201103', '201104', '201105']:
            logging.info(f"Skipping {file_name} for {args.month} due to improper schema/values.")
            continue

        logging.info(f"File: {file_name}")
        
        # Target Elasticsearch index name
        index_name = f"aggregated-{file_name}-{args.month}".replace("_", "-")
        logging.info(f"Index name: {index_name}")
        
        # Delete the existing index if it exists
        delete_existing_index(es, index_name)
        es.indices.create(index=index_name, ignore=400)
        
        # Use the bulk helper to index data from generator
        try:
            helpers.bulk(es, generate_actions(file_path, index_name))
            logging.info(f"Data ingestion completed successfully for index: {index_name}.")
            
            # Refresh the index to make the indexed documents searchable
            es.indices.refresh(index=index_name)
            logging.info(f"Index: {index_name} refreshed to make indexed documents searchable.")

        except Exception as e:
            logging.info(f"An error occurred during ingestion: {e}")

    logging.info("Ingestion process completed.")


if __name__ == "__main__":
    main()
