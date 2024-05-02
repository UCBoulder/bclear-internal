# B-Clear Project

## Installation and project setup steps
- Have a elasticsearch cluster setup on cloud, and update the username and password in the security.ini file to this folder.
- create a local virtualenv - ```virtualenv -p python3 venv```
- activate the env - ```source venv/bin/activate```
- install the python client libraries using the command ```pip install -r requirements.txt```
- download the raw data to validate and upload to elasticsearch. link - https://www.fcc.gov/oet/mba/raw-data-releases. The raw data is already present on the kibana instance at path `~/BMCP/.` in an archived format for each year.

## Folder structure

```
b-clear
    - 202201
    - 202202
    .. data directories
    ..
    filtering_scripts
        *scripts*
    unit_profile_files
        *10 years for unit-profile files*
```


## generate combined unit-profile files 

- download all the unit-profile files from back in 2011. link - https://www.fcc.gov/general/measuring-broadband-america-measuring-fixed-broadband
-  run the script - `combine_profile_files.py` to get a combined unit-profile file later used for ISP mapping

## download certs for elasticsearch access
- update the username and password in security.ini file
- copy the http_ca.crt file from an elasticsearch server. the filepath for it would be /etc/elasticsearch/config/certs/http_ca.crt 
- Create the index template on elasticsearch cluster. See the API query in [tables_mapping.json](tables_mapping.json) file. If adding new fields to the dataset then update the template in the file before pushing the new data. 


## steps to run the pipeline 

once all the files are downloaded and setup as defined in the folder structure above run the bash script `run_pipeline.sh`, it takes the data folder path as an argument. example usage - ```./run_pipeline.sh '../202201'```   

# Steps that are run by the pipeline script 

**Step 1: perform operator identification.**

since the raw data only has unit_ids and no information on ISP and ISP technology the `operator_identification.py` script forms this mapping. The steps it takes are as follows: 

- Firstly, group all the on-net tests associated with each ISP. This is done on the basis of target server.
- Label all the unique unit_ids in a ISP group as belonging to that ISP. 
- make a combined unit-profile file by merging unit-profile and excluded-units file.
- label any unclassified units based on the combined unit-profile file of that year.
- Use the technical report of the year to classify all ISPs belonging to a single technology for that year.
- label any unclassified units based on the combined unit-profile file of that year.
- For any remaining unclassified units use the combined unit-profile file for that year to determine the speed tier for each technology. Calculate the average speed for each unit using the curr_httpgetmt.csv table. Based on the speed tier and average speed classify the technology. For example in the yr'21 4 ISPs used different technology and the speed tier for them was [('CenturyLink', 150), ('Hawaiian Telecom', 110), ('Frontier', 30), ('Cincinnati Bell', 110)], so for CenturyLink <150 would be classified as DSL and more than it would be Fiber.


<!-- - For classification of technology all ISPs other than CenturyLink, Frontier and Hawaiian Telecom run on a single technology for the yr 21 onwards. 
- Use the unit-profile file of the year to further classify any unclassified unit_ids
- For the 3 ISP calculate the monthly average speed for each unit_id from the curr_httpmt table and based on the following speed cut-offs classify as DSL or Fiber. the cut-off speed based on the 2021 unit-profile file is [('CenturyLink', 150), ('Hawaiian Telecom', 110), ('Frontier', 30)]
- <TODO> figuring out the technology of for older data. -->
<br/><br/>

**Step 2: Run the Filters to validate the raw data**

This step is performed by the script filter_data.py, the filters applied for each tables are configurable in the constants.py file. The filters applied for the 4 latency based tables are: 

```py
FILE_FILTER_MAP = {
    "curr_udplatency.csv": ["zeros", "delta_rtt>300ms", "rtt_min<0.5ms", "less_than_50_samples", "packet_loss_10"],
    "curr_ping.csv": ["zeros", "delta_rtt>300ms", "rtt_min<0.5ms", "packet_loss_10"],
    "curr_dlping.csv": ["zeros", "rtt_min<0.5ms", "less_than_50_samples", "packet_loss_10"],
    "curr_ulping.csv": ["zeros", "rtt_min<0.5ms", "less_than_50_samples", "packet_loss_10"]
}
```

The definition for each filter is
- Filter-1 **zeros**: check for 0 and empty values for all the columns
- Filter-2 (FCC) **delta_rtt>300ms**: remove test instances where the range of a unit’s of individual round trip times exceeded 300ms. Formula: $$rtt\_max - rtt\_min > 300000$$
- Filter-3 (FCC) **rtt_min<0.5ms**: remove test instances where any round trip time was reported as 0.05ms or lower. Formula: $$rtt\_min < 50$$ 
- Filter-4 (FCC) **less_than_50_samples**: remove test instances with less than 50 successful packets. Formula: $$successes< 50$$ 
- Filter-5 (FCC) **packet_loss_10**: remove test instances where packet loss exceeded 10% . Formula: $$failures/(successes + failures) > 0.1$$
- Filter-6 (FCC): remove unit_ids in the excluded_unit_ids file at https://data.fcc.gov/download/measuring-broadband-america/2022/units-excluded-sept2021.xlsx

The script creates a new folder `validated_data` later used to ingest data to elasticsearch.
An additional task of this script is to classify each test as either an on-net vs off-net test and add a new column in the raw data. It also adds the fields of ISP name and technology by merging the raw data with the results of the previous step.
<br/><br/>

**Step 3: Ingest validated data into elasticsearch** 

This step is performed by the script `bulk_upload.py`, it requires the security.ini file to access the elastic cloud cluster. The script spawns 4 threads and does an parallel bulk push of 500 documents for each thread at a time.

To create the visualizations and dashboards in Kibana go to the menu and select Stack Management -> Saved Objects and click on the Import button located in the top-right and upload the export.ndjson file in this repo. 