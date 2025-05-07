# Data - Download and Preprocessing

The raw data in [FCC MBA Website]() comes as tar files at a monthly basis. Below are the instructions to download them and process their headers and finally a program to convert them into parquet files for efficient storage.

# Downloading data

`get_data.sh` in helpers folder can be used to download and extract the data. The years to be downloaded should be mentioned in the main function. If there is a space constraint - download, extract and preprocess the data year by year (advised). After extraction and preprocssing, the total size should be around **340GB** for all data from 2011 to 2023 in parquet format. The intermediate steps will consume large spaces (45 GB approx. for years with lots of data - 2011 to 2015 and lesser for other). Hence plan accordingly.


## Data-preprocessing

### 2011

#### 02 to 06
1. Remove # for the headers
2. Add header to curr_videostream

```bash
#!/bin/bash
# Usage: ./remove_header_hash.sh [directory]
# Program to remove # from the headers

# If no directory is provided, the current directory is used.
# Set the target directory (default to current directory)
DIRECTORY="${1:-.}"

# Loop through each CSV file in the directory
for file in "$DIRECTORY"/*.csv; do
	if [[ -f "$file" ]]; then
		# Remove a leading '#' from the first line only
		sed -i '1s/^#//' "$file"
		echo "Processed $file"
	fi
done
```

```bash
# CHECK AFTER INSERTION
sed -i 1i'unit_id,dtime,target,downthrpt,downjitter,latency,jitter,buffer_underruns,buffer_delay,buffer_filltime,duration,bitrate,buffer_size,successes,failures,location_id' ./curr_videostream.csv
```

#### 07 to 12
1. Add header for all files
2. Webget file headers different for month 07, 08 from the rest (See the commands for webget)

Headers:

```bash
# Change file name
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httpgetmt_2011_12.csv

sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httppostmt_2011_12.csv

sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_ping_2011_12.csv

sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_dlping_2011_12.csv

sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_ulping_2011_12.csv

sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_udplatency_2011_12.csv

sed -i 1i'unit_id,dtime,target,downthrpt,downjitter,latency,jitter,buffer_underruns,buffer_delay,buffer_filltime,duration,bitrate,buffer_size,successes,failures,location_id' ./curr_videostream_2011_12.csv

sed -i 1i'unit_id,dtime,target,packet_size,stream_rate,duration,packets_up_sent,packets_down_sent,packets_up_recv,packets_down_recv,jitter_up,jitter_down,latency,successes,failures,location_id' ./curr_udpjitter_2011_12.csv

# Webget is different
# For 2011, months 07, 08
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,objects,successes,failures,location_id' ./curr_webget_2011_08.csv
# For the rest
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,objects,threads,requests,connections,reused_connections,lookups,request_total_time,request_min_time,request_avg_time,request_max_time,ttfb_total_time,ttfb_min_time,ttfb_avg_time,ttfb_max_time,lookup_total_time,lookup_min_time,lookup_avg_time,lookup_max_time,successes,failures,location_id' ./curr_webget_2011_12.csv

sed -i 1i'unit_id,dtime,nameserver,lookup_host,response_ip,rtt,successes,failures,location_id' ./curr_dns_2011_12.csv

sed -i 1i'unit_id,location_id,dtime,successes,failures' ./curr_avail_2011_12.csv

sed -i 1i'unit_id,dtime,wan_rx_bytes,wan_tx_bytes,sk_rx_bytes,sk_tx_bytes,location_id' ./curr_netusage_2011_12.csv
```

### 2012 to 2016 (Aug)

From 2012 to the august of 2016, there are some files that get added/removed each year. Regardless, all of them lack headers. This python code can be used to print headers for all the files given the year and month in a specific format (`suffix`). This can also be used in a program to automate the header insertion.

```python

suffix = "_yyyy_mm"

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httpget{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httpgetmt{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httpgetmt6{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httppost{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httppostmt{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ./curr_httppostmt6{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,protocol,hop,hop_address,hop_name,sent,received,rtt_avg,successes,failures,location_id' ./curr_traceroute{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_ping{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_dlping{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_ulping{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,duration,target,address,packets,location_id' ./curr_udpcloss{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_udplatency{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ./curr_udplatency6{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,downthrpt,downjitter,latency,jitter,buffer_underruns,buffer_delay,buffer_filltime,duration,bitrate,buffer_size,successes,failures,location_id' ./curr_videostream{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,packet_size,stream_rate,duration,packets_up_sent,packets_down_sent,packets_up_recv,packets_down_recv,jitter_up,jitter_down,latency,successes,failures,location_id' ./curr_udpjitter{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,objects,threads,requests,connections,reused_connections,lookups,request_total_time,request_min_time,request_avg_time,request_max_time,ttfb_total_time,ttfb_min_time,ttfb_avg_time,ttfb_max_time,lookup_total_time,lookup_min_time,lookup_avg_time,lookup_max_time,successes,failures,location_id' ./curr_webget{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,nameserver,lookup_host,response_ip,rtt,successes,failures,location_id' ./curr_dns{suffix}.csv")

print(f"sed -i 1i'unit_id,location_id,dtime,successes,failures' ./curr_avail{suffix}.csv")

print(f"sed -i 1i'unit_id,dtime,wan_rx_bytes,wan_tx_bytes,sk_rx_bytes,sk_tx_bytes,location_id' ./curr_netusage{suffix}.csv")

```

### 2016 Sept to 2023

All the files have headers in them.
In 2021, 
	month 04: `curr_traceroute.csv` has extra headers and `curr_webget.csv` has erroneous first line
	month 05 and 06: `curr_traceroute.csv`, `curr_dns.csv` and `curr_webget.csv`  has extra headers

## Converting to parquet

Use `convert.py` in helpers folder to convert a folder containing csv files to another folder containing corresponding parquet files.