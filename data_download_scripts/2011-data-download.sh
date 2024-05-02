#!/bin/bash


curl -L https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-feb-2011.tar.gz > raw-bulk-data-feb-2011.tar.gz
mkdir 201102 ../201102
tar -xf raw-bulk-data-feb-2011.tar.gz -C 201102/.
mv 201102/curr_httppostmt.csv ../201102/curr_httppostmt.csv
mv 201102/curr_httpgettmt.csv ../201102/curr_httpgettmt.csv
mv 201102/curr_udplatency.csv ../201102/curr_udplatency.csv
mv 201102/curr_ping.csv ../201102/curr_ping.csv
mv 201102/curr_ulping.csv ../201102/curr_ulping.csv
mv 201102/curr_dlping.csv ../201102/curr_dlping.csv
rm -rf 201102 raw-bulk-data-feb-2011.tar.gz
sed -i '1 d' ../201102/curr_httppostmt.csv
sed -i '1 d' ../201102/curr_httpgetmt.csv
sed -i '1 d' ../201102/curr_ping.csv
sed -i '1 d' ../201102/curr_dlping.csv
sed -i '1 d' ../201102/curr_ulping.csv
sed -i '1 d' ../201102/curr_udplatency.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201102/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201102/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201102/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201102/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201102/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201102/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-mar-2011.tar.gz > raw-bulk-data-mar-2011.tar.gz
mkdir 201103 ../201103
tar -xf raw-bulk-data-mar-2011.tar.gz -C 201103/.
mv 201103/curr_httppostmt.csv ../201103/curr_httppostmt.csv
mv 201103/curr_httpgettmt.csv ../201103/curr_httpgettmt.csv
mv 201103/curr_udplatency.csv ../201103/curr_udplatency.csv
mv 201103/curr_ping.csv ../201103/curr_ping.csv
mv 201103/curr_ulping.csv ../201103/curr_ulping.csv
mv 201103/curr_dlping.csv ../201103/curr_dlping.csv
rm -rf 201103 raw-bulk-data-mar-2011.tar.gz
sed -i '1 d' ../201103/curr_httppostmt.csv
sed -i '1 d' ../201103/curr_httpgetmt.csv
sed -i '1 d' ../201103/curr_ping.csv
sed -i '1 d' ../201103/curr_dlping.csv
sed -i '1 d' ../201103/curr_ulping.csv
sed -i '1 d' ../201103/curr_udplatency.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201103/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201103/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201103/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201103/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201103/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201103/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-apr-2011.tar.gz > raw-bulk-data-apr-2011.tar.gz
mkdir 201104 ../201104
tar -xf raw-bulk-data-apr-2011.tar.gz -C 201104/.
mv 201104/curr_httppostmt.csv ../201104/curr_httppostmt.csv
mv 201104/curr_httpgettmt.csv ../201104/curr_httpgettmt.csv
mv 201104/curr_udplatency.csv ../201104/curr_udplatency.csv
mv 201104/curr_ping.csv ../201104/curr_ping.csv
mv 201104/curr_ulping.csv ../201104/curr_ulping.csv
mv 201104/curr_dlping.csv ../201104/curr_dlping.csv
rm -rf 201104 raw-bulk-data-apr-2011.tar.gz
sed -i '1 d' ../201104/curr_httppostmt.csv
sed -i '1 d' ../201104/curr_httpgetmt.csv
sed -i '1 d' ../201104/curr_ping.csv
sed -i '1 d' ../201104/curr_dlping.csv
sed -i '1 d' ../201104/curr_ulping.csv
sed -i '1 d' ../201104/curr_udplatency.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201104/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201104/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201104/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201104/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201104/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201104/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-may-2011.tar.gz > raw-bulk-data-may-2011.tar.gz
mkdir 201105 ../201105
tar -xf raw-bulk-data-may-2011.tar.gz -C 201105/.
mv 201105/curr_httppostmt.csv ../201105/curr_httppostmt.csv
mv 201105/curr_httpgettmt.csv ../201105/curr_httpgettmt.csv
mv 201105/curr_udplatency.csv ../201105/curr_udplatency.csv
mv 201105/curr_ping.csv ../201105/curr_ping.csv
mv 201105/curr_ulping.csv ../201105/curr_ulping.csv
mv 201105/curr_dlping.csv ../201105/curr_dlping.csv
rm -rf 201105 raw-bulk-data-may-2011.tar.gz
sed -i '1 d' ../201105/curr_httppostmt.csv
sed -i '1 d' ../201105/curr_httpgetmt.csv
sed -i '1 d' ../201105/curr_ping.csv
sed -i '1 d' ../201105/curr_dlping.csv
sed -i '1 d' ../201105/curr_ulping.csv
sed -i '1 d' ../201105/curr_udplatency.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201105/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201105/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201105/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201105/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201105/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201105/curr_udplatency.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/raw-bulk-data-jun-2011.tar.gz > raw-bulk-data-jun-2011.tar.gz
mkdir 201106 ../201106
tar -xf raw-bulk-data-jun-2011.tar.gz -C 201106/.
mv 201106/curr_httppostmt.csv ../201106/curr_httppostmt.csv
mv 201106/curr_httpgettmt.csv ../201106/curr_httpgettmt.csv
mv 201106/curr_udplatency.csv ../201106/curr_udplatency.csv
mv 201106/curr_ping.csv ../201106/curr_ping.csv
mv 201106/curr_ulping.csv ../201106/curr_ulping.csv
mv 201106/curr_dlping.csv ../201106/curr_dlping.csv
rm -rf 201106 raw-bulk-data-jun-2011.tar.gz
sed -i '1 d' ../201106/curr_httppostmt.csv
sed -i '1 d' ../201106/curr_httpgetmt.csv
sed -i '1 d' ../201106/curr_ping.csv
sed -i '1 d' ../201106/curr_dlping.csv
sed -i '1 d' ../201106/curr_ulping.csv
sed -i '1 d' ../201106/curr_udplatency.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201106/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,fetch_time,bytes_total,bytes_sec,warmup_time,warmup_bytes,sequence,threads,successes,failures' ../201106/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201106/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201106/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201106/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures' ../201106/curr_udplatency.csv


curl -L https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201107.tar.gz > raw-bulk-data-jul-2011.tar.gz
mkdir ../201107
tar -xf raw-bulk-data-jul-2011.tar.gz
mv 201107/curr_httppostmt_2011_07.csv ../201107/curr_httppostmt.csv
mv 201107/curr_httpgettmt_2011_07.csv ../201107/curr_httpgettmt.csv
mv 201107/curr_udplatency_2011_07.csv ../201107/curr_udplatency.csv
mv 201107/curr_ping_2011_07.csv ../201107/curr_ping.csv
mv 201107/curr_ulping_2011_07.csv ../201107/curr_ulping.csv
mv 201107/curr_dlping_2011_07.csv ../201107/curr_dlping.csv
rm -rf 201107 raw-bulk-data-jul-2011.tar.gz
sed -i '1 d' ../201107/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201107/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201107/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201107/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201107/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201107/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201107/curr_udplatency.csv


curl -L https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201108.tar.gz > raw-bulk-data-aug-2011.tar.gz
mkdir ../201108
tar -xf raw-bulk-data-aug-2011.tar.gz
mv 201108/curr_httppostmt_2011_08.csv ../201108/curr_httppostmt.csv
mv 201108/curr_httpgettmt_2011_08.csv ../201108/curr_httpgettmt.csv
mv 201108/curr_udplatency_2011_08.csv ../201108/curr_udplatency.csv
mv 201108/curr_ping_2011_08.csv ../201108/curr_ping.csv
mv 201108/curr_ulping_2011_08.csv ../201108/curr_ulping.csv
mv 201108/curr_dlping_2011_08.csv ../201108/curr_dlping.csv
rm -rf 201108 raw-bulk-data-aug-2011.tar.gz
sed -i '1 d' ../201108/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201108/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201108/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201108/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201108/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201108/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201108/curr_udplatency.csv


curl -L https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201109.tar.gz > raw-bulk-data-sept-2011.tar.gz
mkdir ../201109
tar -xf raw-bulk-data-sept-2011.tar.gz
mv 201109/curr_httppostmt_2011_09.csv ../201109/curr_httppostmt.csv
mv 201109/curr_httpgettmt_2011_09.csv ../201109/curr_httpgettmt.csv
mv 201109/curr_udplatency_2011_09.csv ../201109/curr_udplatency.csv
mv 201109/curr_ping_2011_09.csv ../201109/curr_ping.csv
mv 201109/curr_ulping_2011_09.csv ../201109/curr_ulping.csv
mv 201109/curr_dlping_2011_09.csv ../201109/curr_dlping.csv
rm -rf 201109 raw-bulk-data-sept-2011.tar.gz
sed -i '1 d' ../201109/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201109/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201109/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201109/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201109/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201109/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201109/curr_udplatency.csv


curl -L https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201110.tar.gz > raw-bulk-data-oct-2011.tar.gz
mkdir ../201110
tar -xf raw-bulk-data-oct-2011.tar.gz
mv 201110/curr_httppostmt_2011_10.csv ../201110/curr_httppostmt.csv
mv 201110/curr_httpgettmt_2011_10.csv ../201110/curr_httpgettmt.csv
mv 201110/curr_udplatency_2011_10.csv ../201110/curr_udplatency.csv
mv 201110/curr_ping_2011_10.csv ../201110/curr_ping.csv
mv 201110/curr_ulping_2011_10.csv ../201110/curr_ulping.csv
mv 201110/curr_dlping_2011_10.csv ../201110/curr_dlping.csv
rm -rf 201110 raw-bulk-data-oct-2011.tar.gz
sed -i '1 d' ../201110/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201110/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201110/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201110/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201110/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201110/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201110/curr_udplatency.csv


curl -L https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201111.tar.gz > raw-bulk-data-nov-2011.tar.gz
mkdir ../201111
tar -xf raw-bulk-data-nov-2011.tar.gz
mv 201111/curr_httppostmt_2011_11.csv ../201111/curr_httppostmt.csv
mv 201111/curr_httpgettmt_2011_11.csv ../201111/curr_httpgettmt.csv
mv 201111/curr_udplatency_2011_11.csv ../201111/curr_udplatency.csv
mv 201111/curr_ping_2011_11.csv ../201111/curr_ping.csv
mv 201111/curr_ulping_2011_11.csv ../201111/curr_ulping.csv
mv 201111/curr_dlping_2011_11.csv ../201111/curr_dlping.csv
rm -rf 201111 raw-bulk-data-nov-2011.tar.gz
sed -i '1 d' ../201111/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201111/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201111/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201111/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201111/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201111/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201111/curr_udplatency.csv


curl -L https://s3.amazonaws.com/fcc-april-data/fcc_2012_raw/fcc_201112.tar.gz > raw-bulk-data-dec-2011.tar.gz
mkdir ../201112
tar -xf raw-bulk-data-dec-2011.tar.gz
mv 201112/curr_httppostmt_2011_12.csv ../201112/curr_httppostmt.csv
mv 201112/curr_httpgettmt_2011_12.csv ../201112/curr_httpgettmt.csv
mv 201112/curr_udplatency_2011_12.csv ../201112/curr_udplatency.csv
mv 201112/curr_ping_2011_12.csv ../201112/curr_ping.csv
mv 201112/curr_ulping_2011_12.csv ../201112/curr_ulping.csv
mv 201112/curr_dlping_2011_12.csv ../201112/curr_dlping.csv
rm -rf 201112 raw-bulk-data-dec-2011.tar.gz
sed -i '1 d' ../201112/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201112/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201112/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201112/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201112/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201112/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201112/curr_udplatency.csv
