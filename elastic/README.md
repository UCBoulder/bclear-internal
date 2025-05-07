# Elastic Folder

This folder contains configuration and security assets for connecting to Elasticsearch.

## Contents

- `http_ca.crt`  
  CA certificate for verifying SSL connections to the Elasticsearch cluster.

- `security.ini`  
  INI-style configuration file defining credentials and your certificate path.

  ```ini
  [ELASTIC]
  user = elastic
  password = <your-password>
  ca_cert = http_ca.crt
  ```

## Usage

* Referenced by `ingest.py` (project root) to establish a secure connection with the elastic database:

  ```python
  config = configparser.ConfigParser()
  config.read("elastic/security.ini")
  es, status = get_client(config)
  ```

* **Keep these files secure**; do not commit real credentials to public repos.

## Customization

* Update `security.ini` with your Elasticsearch username/password.
* If your cluster’s CA certificate changes, replace `http_ca.crt` accordingly.

