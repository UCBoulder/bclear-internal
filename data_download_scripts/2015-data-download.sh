#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-jan.tar.gz > data-raw-2015-jan.tar.gz
tar -xf data-raw-2015-jan.tar.gz
mkdir -p ../201501
mv 201501/curr_httppostmt_2015_01.csv ../201501/curr_httppostmt.csv
mv 201501/curr_httpgetmt_2015_01.csv ../201501/curr_httpgetmt.csv
mv 201501/curr_udplatency_2015_01.csv ../201501/curr_udplatency.csv
mv 201501/curr_ping_2015_01.csv ../201501/curr_ping.csv
mv 201501/curr_dlping_2015_01.csv ../201501/curr_dlping.csv
mv 201501/curr_ulping_2015_01.csv ../201501/curr_ulping.csv
rm -rf 201501 data-raw-2015-jan.tar.gz
sed -i '1 d' ../201501/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201501/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201501/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201501/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201501/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201501/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201501/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-feb.tar.gz > data-raw-2015-feb.tar.gz
tar -xf data-raw-2015-feb.tar.gz
mkdir -p ../201502
mv 201502/curr_httppostmt_2015_02.csv ../201502/curr_httppostmt.csv
mv 201502/curr_httpgetmt_2015_02.csv ../201502/curr_httpgetmt.csv
mv 201502/curr_udplatency_2015_02.csv ../201502/curr_udplatency.csv
mv 201502/curr_ping_2015_02.csv ../201502/curr_ping.csv
mv 201502/curr_dlping_2015_02.csv ../201502/curr_dlping.csv
mv 201502/curr_ulping_2015_02.csv ../201502/curr_ulping.csv
rm -rf 201502 data-raw-2015-feb.tar.gz
sed -i '1 d' ../201502/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201502/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201502/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201502/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201502/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201502/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201502/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-mar.tar.gz > data-raw-2015-mar.tar.gz
tar -xf data-raw-2015-mar.tar.gz
mkdir -p ../201503
mv 201503/curr_httppostmt_2015_03.csv ../201503/curr_httppostmt.csv
mv 201503/curr_httpgetmt_2015_03.csv ../201503/curr_httpgetmt.csv
mv 201503/curr_udplatency_2015_03.csv ../201503/curr_udplatency.csv
mv 201503/curr_ping_2015_03.csv ../201503/curr_ping.csv
mv 201503/curr_dlping_2015_03.csv ../201503/curr_dlping.csv
mv 201503/curr_ulping_2015_03.csv ../201503/curr_ulping.csv
rm -rf 201503 data-raw-2015-mar.tar.gz
sed -i '1 d' ../201503/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201503/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201503/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201503/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201503/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201503/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201503/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-apr.tar.gz > data-raw-2015-apr.tar.gz
tar -xf data-raw-2015-apr.tar.gz
mkdir -p ../201504
mv 201504/curr_httppostmt_2015_04.csv ../201504/curr_httppostmt.csv
mv 201504/curr_httpgetmt_2015_04.csv ../201504/curr_httpgetmt.csv
mv 201504/curr_udplatency_2015_04.csv ../201504/curr_udplatency.csv
mv 201504/curr_ping_2015_04.csv ../201504/curr_ping.csv
mv 201504/curr_dlping_2015_04.csv ../201504/curr_dlping.csv
mv 201504/curr_ulping_2015_04.csv ../201504/curr_ulping.csv
rm -rf 201504 data-raw-2015-apr.tar.gz
sed -i '1 d' ../201504/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201504/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201504/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201504/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201504/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201504/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201504/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-may.tar.gz > data-raw-2015-may.tar.gz
tar -xf data-raw-2015-may.tar.gz
mkdir -p ../201505
mv 201505/curr_httppostmt_2015_05.csv ../201505/curr_httppostmt.csv
mv 201505/curr_httpgetmt_2015_05.csv ../201505/curr_httpgetmt.csv
mv 201505/curr_udplatency_2015_05.csv ../201505/curr_udplatency.csv
mv 201505/curr_ping_2015_05.csv ../201505/curr_ping.csv
mv 201505/curr_dlping_2015_05.csv ../201505/curr_dlping.csv
mv 201505/curr_ulping_2015_05.csv ../201505/curr_ulping.csv
rm -rf 201505 data-raw-2015-may.tar.gz
sed -i '1 d' ../201505/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201505/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201505/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201505/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201505/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201505/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201505/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-june.tar.gz > data-raw-2015-june.tar.gz
tar -xf data-raw-2015-june.tar.gz
mkdir -p ../201506
mv 201506/curr_httppostmt_2015_06.csv ../201506/curr_httppostmt.csv
mv 201506/curr_httpgetmt_2015_06.csv ../201506/curr_httpgetmt.csv
mv 201506/curr_udplatency_2015_06.csv ../201506/curr_udplatency.csv
mv 201506/curr_ping_2015_06.csv ../201506/curr_ping.csv
mv 201506/curr_dlping_2015_06.csv ../201506/curr_dlping.csv
mv 201506/curr_ulping_2015_06.csv ../201506/curr_ulping.csv
rm -rf 201506 data-raw-2015-june.tar.gz
sed -i '1 d' ../201506/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201506/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201506/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201506/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201506/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201506/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201506/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-july.tar.gz > data-raw-2015-july.tar.gz
tar -xf data-raw-2015-july.tar.gz
mkdir -p ../201507
mv 201507/curr_httppostmt_2015_07.csv ../201507/curr_httppostmt.csv
mv 201507/curr_httpgetmt_2015_07.csv ../201507/curr_httpgetmt.csv
mv 201507/curr_udplatency_2015_07.csv ../201507/curr_udplatency.csv
mv 201507/curr_ping_2015_07.csv ../201507/curr_ping.csv
mv 201507/curr_dlping_2015_07.csv ../201507/curr_dlping.csv
mv 201507/curr_ulping_2015_07.csv ../201507/curr_ulping.csv
rm -rf 201507 data-raw-2015-july.tar.gz
sed -i '1 d' ../201507/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201507/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201507/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201507/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201507/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201507/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201507/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-aug.tar.gz > data-raw-2015-aug.tar.gz
tar -xf data-raw-2015-aug.tar.gz
mkdir -p ../201508
mv 201508/curr_httppostmt_2015_08.csv ../201508/curr_httppostmt.csv
mv 201508/curr_httpgetmt_2015_08.csv ../201508/curr_httpgetmt.csv
mv 201508/curr_udplatency_2015_08.csv ../201508/curr_udplatency.csv
mv 201508/curr_ping_2015_08.csv ../201508/curr_ping.csv
mv 201508/curr_dlping_2015_08.csv ../201508/curr_dlping.csv
mv 201508/curr_ulping_2015_08.csv ../201508/curr_ulping.csv
rm -rf 201508 data-raw-2015-aug.tar.gz
sed -i '1 d' ../201508/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201508/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201508/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201508/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201508/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201508/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201508/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-sept.tar.gz > data-raw-2015-sept.tar.gz
tar -xf data-raw-2015-sept.tar.gz
mkdir -p ../201509
mv 201509/curr_httppostmt_2015_09.csv ../201509/curr_httppostmt.csv
mv 201509/curr_httpgetmt_2015_09.csv ../201509/curr_httpgetmt.csv
mv 201509/curr_udplatency_2015_09.csv ../201509/curr_udplatency.csv
mv 201509/curr_ping_2015_09.csv ../201509/curr_ping.csv
mv 201509/curr_dlping_2015_09.csv ../201509/curr_dlping.csv
mv 201509/curr_ulping_2015_09.csv ../201509/curr_ulping.csv
rm -rf 201509 data-raw-2015-sept.tar.gz
sed -i '1 d' ../201509/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201509/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201509/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201509/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201509/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201509/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201509/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-oct.tar.gz > data-raw-2015-oct.tar.gz
tar -xf data-raw-2015-oct.tar.gz
mkdir -p ../201510
mv 201510/curr_httppostmt_2015_10.csv ../201510/curr_httppostmt.csv
mv 201510/curr_httpgetmt_2015_10.csv ../201510/curr_httpgetmt.csv
mv 201510/curr_udplatency_2015_10.csv ../201510/curr_udplatency.csv
mv 201510/curr_ping_2015_10.csv ../201510/curr_ping.csv
mv 201510/curr_dlping_2015_10.csv ../201510/curr_dlping.csv
mv 201510/curr_ulping_2015_10.csv ../201510/curr_ulping.csv
rm -rf 201510 data-raw-2015-oct.tar.gz
sed -i '1 d' ../201510/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201510/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201510/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201510/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201510/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201510/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201510/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-nov.tar.gz > data-raw-2015-nov.tar.gz
tar -xf data-raw-2015-nov.tar.gz
mkdir -p ../201511
mv 201511/curr_httppostmt_2015_11.csv ../201511/curr_httppostmt.csv
mv 201511/curr_httpgetmt_2015_11.csv ../201511/curr_httpgetmt.csv
mv 201511/curr_udplatency_2015_11.csv ../201511/curr_udplatency.csv
mv 201511/curr_ping_2015_11.csv ../201511/curr_ping.csv
mv 201511/curr_dlping_2015_11.csv ../201511/curr_dlping.csv
mv 201511/curr_ulping_2015_11.csv ../201511/curr_ulping.csv
rm -rf 201511 data-raw-2015-nov.tar.gz
sed -i '1 d' ../201511/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201511/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201511/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201511/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201511/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201511/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201511/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2016/data-raw-2015-dec.tar.gz > data-raw-2015-dec.tar.gz
tar -xf data-raw-2015-dec.tar.gz
mkdir -p ../201512
mv 201512/curr_httppostmt_2015_12.csv ../201512/curr_httppostmt.csv
mv 201512/curr_httpgetmt_2015_12.csv ../201512/curr_httpgetmt.csv
mv 201512/curr_udplatency_2015_12.csv ../201512/curr_udplatency.csv
mv 201512/curr_ping_2015_12.csv ../201512/curr_ping.csv
mv 201512/curr_dlping_2015_12.csv ../201512/curr_dlping.csv
mv 201512/curr_ulping_2015_12.csv ../201512/curr_ulping.csv
rm -rf 201512 data-raw-2015-dec.tar.gz
sed -i '1 d' ../201512/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201512/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201512/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201512/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201512/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201512/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201512/curr_udplatency.csv