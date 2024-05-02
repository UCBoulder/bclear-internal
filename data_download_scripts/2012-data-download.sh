#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-jan.tar.gz > data-raw-2012-jan.tar.gz
tar -xf data-raw-2012-jan.tar.gz
mkdir ../201201
mv 201201/curr_httppostmt_2012_01.csv ../201201/curr_httppostmt.csv
mv 201201/curr_httpgettmt_2012_01.csv ../201201/curr_httpgettmt.csv
mv 201201/curr_udplatency_2012_01.csv ../201201/curr_udplatency.csv
mv 201201/curr_ping_2012_01.csv ../201201/curr_ping.csv
mv 201201/curr_ulping_2012_01.csv ../201201/curr_ulping.csv
mv 201201/curr_dlping_2012_01.csv ../201201/curr_dlping.csv
rm -rf 201201 data-raw-2012-jan.tar.gz
sed -i '1 d' ../201201/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201201/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201201/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-feb.tar.gz > data-raw-2012-feb.tar.gz
tar -xf data-raw-2012-feb.tar.gz
mkdir ../201202
mv 201202/curr_httppostmt_2012_01.csv ../201202/curr_httppostmt.csv
mv 201202/curr_httpgettmt_2012_01.csv ../201202/curr_httpgettmt.csv
mv 201202/curr_udplatency_2012_01.csv ../201202/curr_udplatency.csv
mv 201202/curr_ping_2012_01.csv ../201202/curr_ping.csv
mv 201202/curr_ulping_2012_01.csv ../201202/curr_ulping.csv
mv 201202/curr_dlping_2012_01.csv ../201202/curr_dlping.csv
rm -rf 201202 data-raw-2012-feb.tar.gz
sed -i '1 d' ../201202/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201202/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201202/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-mar.tar.gz > data-raw-2012-mar.tar.gz
tar -xf data-raw-2012-mar.tar.gz
mkdir ../201203
mv 201203/curr_httppostmt_2012_01.csv ../201203/curr_httppostmt.csv
mv 201203/curr_httpgettmt_2012_01.csv ../201203/curr_httpgettmt.csv
mv 201203/curr_udplatency_2012_01.csv ../201203/curr_udplatency.csv
mv 201203/curr_ping_2012_01.csv ../201203/curr_ping.csv
mv 201203/curr_ulping_2012_01.csv ../201203/curr_ulping.csv
mv 201203/curr_dlping_2012_01.csv ../201203/curr_dlping.csv
rm -rf 201203 data-raw-2012-mar.tar.gz
sed -i '1 d' ../201203/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201203/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201203/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-apr.tar.gz > data-raw-2012-apr.tar.gz
tar -xf data-raw-2012-apr.tar.gz
mkdir ../201204
mv 201204/curr_httppostmt_2012_04.csv ../201204/curr_httppostmt.csv
mv 201204/curr_httpgettmt_2012_04.csv ../201204/curr_httpgettmt.csv
mv 201204/curr_udplatency_2012_04.csv ../201204/curr_udplatency.csv
mv 201204/curr_ping_2012_04.csv ../201204/curr_ping.csv
mv 201204/curr_ulping_2012_04.csv ../201204/curr_ulping.csv
mv 201204/curr_dlping_2012_04.csv ../201204/curr_dlping.csv
rm -rf 201204 data-raw-2012-apr.tar.gz
sed -i '1 d' ../201204/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201204/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201204/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-may.tar.gz > data-raw-2012-may.tar.gz
tar -xf data-raw-2012-may.tar.gz
mkdir ../201205
mv 201205/curr_httppostmt_2012_05.csv ../201205/curr_httppostmt.csv
mv 201205/curr_httpgettmt_2012_05.csv ../201205/curr_httpgettmt.csv
mv 201205/curr_udplatency_2012_05.csv ../201205/curr_udplatency.csv
mv 201205/curr_ping_2012_05.csv ../201205/curr_ping.csv
mv 201205/curr_ulping_2012_05.csv ../201205/curr_ulping.csv
mv 201205/curr_dlping_2012_05.csv ../201205/curr_dlping.csv
rm -rf 201205 data-raw-2012-may.tar.gz
sed -i '1 d' ../201205/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201205/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201205/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-june.tar.gz > data-raw-2012-june.tar.gz
tar -xf data-raw-2012-june.tar.gz
mkdir ../201206
mv 201206/curr_httppostmt_2012_06.csv ../201206/curr_httppostmt.csv
mv 201206/curr_httpgettmt_2012_06.csv ../201206/curr_httpgettmt.csv
mv 201206/curr_udplatency_2012_06.csv ../201206/curr_udplatency.csv
mv 201206/curr_ping_2012_06.csv ../201206/curr_ping.csv
mv 201206/curr_ulping_2012_06.csv ../201206/curr_ulping.csv
mv 201206/curr_dlping_2012_06.csv ../201206/curr_dlping.csv
rm -rf 201206 data-raw-2012-june.tar.gz
sed -i '1 d' ../201206/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201206/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201206/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-july.tar.gz > data-raw-2012-july.tar.gz
tar -xf data-raw-2012-july.tar.gz
mkdir ../201207
mv 201207/curr_httppostmt_2012_07.csv ../201207/curr_httppostmt.csv
mv 201207/curr_httpgettmt_2012_07.csv ../201207/curr_httpgettmt.csv
mv 201207/curr_udplatency_2012_07.csv ../201207/curr_udplatency.csv
mv 201207/curr_ping_2012_07.csv ../201207/curr_ping.csv
mv 201207/curr_ulping_2012_07.csv ../201207/curr_ulping.csv
mv 201207/curr_dlping_2012_07.csv ../201207/curr_dlping.csv
rm -rf 201207 data-raw-2012-july.tar.gz
sed -i '1 d' ../201207/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201207/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201207/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-aug.tar.gz > data-raw-2012-aug.tar.gz
tar -xf data-raw-2012-aug.tar.gz
mkdir ../201208
mv 201208/curr_httppostmt_2012_08.csv ../201208/curr_httppostmt.csv
mv 201208/curr_httpgettmt_2012_08.csv ../201208/curr_httpgettmt.csv
mv 201208/curr_udplatency_2012_08.csv ../201208/curr_udplatency.csv
mv 201208/curr_ping_2012_08.csv ../201208/curr_ping.csv
mv 201208/curr_ulping_2012_08.csv ../201208/curr_ulping.csv
mv 201208/curr_dlping_2012_08.csv ../201208/curr_dlping.csv
rm -rf 201208 data-raw-2012-aug.tar.gz
sed -i '1 d' ../201208/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201208/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201208/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-sept.tar.gz > data-raw-2012-sept.tar.gz
tar -xf data-raw-2012-sept.tar.gz
mkdir ../201209
mv 201209/curr_httppostmt_2012_09.csv ../201209/curr_httppostmt.csv
mv 201209/curr_httpgettmt_2012_09.csv ../201209/curr_httpgettmt.csv
mv 201209/curr_udplatency_2012_09.csv ../201209/curr_udplatency.csv
mv 201209/curr_ping_2012_09.csv ../201209/curr_ping.csv
mv 201209/curr_ulping_2012_09.csv ../201209/curr_ulping.csv
mv 201209/curr_dlping_2012_09.csv ../201209/curr_dlping.csv
rm -rf 201209 data-raw-2012-sept.tar.gz
sed -i '1 d' ../201209/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201209/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201209/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-oct.tar.gz > data-raw-2012-oct.tar.gz
tar -xf data-raw-2012-oct.tar.gz
mkdir ../201210
mv 201210/curr_httppostmt_2012_10.csv ../201210/curr_httppostmt.csv
mv 201210/curr_httpgettmt_2012_10.csv ../201210/curr_httpgettmt.csv
mv 201210/curr_udplatency_2012_10.csv ../201210/curr_udplatency.csv
mv 201210/curr_ping_2012_10.csv ../201210/curr_ping.csv
mv 201210/curr_ulping_2012_10.csv ../201210/curr_ulping.csv
mv 201210/curr_dlping_2012_10.csv ../201210/curr_dlping.csv
rm -rf 201210 data-raw-2012-oct.tar.gz
sed -i '1 d' ../201210/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201210/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201210/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-nov.tar.gz > data-raw-2012-nov.tar.gz
tar -xf data-raw-2012-nov.tar.gz
mkdir ../201211
mv 201211/curr_httppostmt_2012_11.csv ../201211/curr_httppostmt.csv
mv 201211/curr_httpgettmt_2012_11.csv ../201211/curr_httpgettmt.csv
mv 201211/curr_udplatency_2012_11.csv ../201211/curr_udplatency.csv
mv 201211/curr_ping_2012_11.csv ../201211/curr_ping.csv
mv 201211/curr_ulping_2012_11.csv ../201211/curr_ulping.csv
mv 201211/curr_dlping_2012_11.csv ../201211/curr_dlping.csv
rm -rf 201211 data-raw-2012-nov.tar.gz
sed -i '1 d' ../201211/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201211/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201211/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2012/data-raw-2012-dec.tar.gz > data-raw-2012-dec.tar.gz
tar -xf data-raw-2012-dec.tar.gz
mkdir ../201212
mv 201212/curr_httppostmt_2012_12.csv ../201212/curr_httppostmt.csv
mv 201212/curr_httpgettmt_2012_12.csv ../201212/curr_httpgettmt.csv
mv 201212/curr_udplatency_2012_12.csv ../201212/curr_udplatency.csv
mv 201212/curr_ping_2012_12.csv ../201212/curr_ping.csv
mv 201212/curr_ulping_2012_12.csv ../201212/curr_ulping.csv
mv 201212/curr_dlping_2012_12.csv ../201212/curr_dlping.csv
rm -rf 201212 data-raw-2012-dec.tar.gz
sed -i '1 d' ../201212/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201212/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201212/curr_httppostmt.csv