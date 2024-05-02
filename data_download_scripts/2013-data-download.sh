#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-jan.tar.gz > data-raw-2013-jan.tar.gz
tar -xf data-raw-2013-jan.tar.gz
mkdir ../201301
mv 201301/curr_httppostmt_2013_01.csv ../201301/curr_httppostmt.csv
mv 201301/curr_httpgettmt_2013_01.csv ../201301/curr_httpgettmt.csv
mv 201301/curr_udplatency_2013_01.csv ../201301/curr_udplatency.csv
mv 201301/curr_ping_2013_01.csv ../201301/curr_ping.csv
mv 201301/curr_ulping_2013_01.csv ../201301/curr_ulping.csv
mv 201301/curr_dlping_2013_01.csv ../201301/curr_dlping.csv
rm -rf 201301 data-raw-2013-jan.tar.gz
sed -i '1 d' ../201301/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201301/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201301/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-feb.tar.gz > data-raw-2013-feb.tar.gz
tar -xf data-raw-2013-feb.tar.gz
mkdir ../201302
mv 201302/curr_httppostmt_2013_02.csv ../201302/curr_httppostmt.csv
mv 201302/curr_httpgettmt_2013_02.csv ../201302/curr_httpgettmt.csv
mv 201302/curr_udplatency_2013_02.csv ../201302/curr_udplatency.csv
mv 201302/curr_ping_2013_02.csv ../201302/curr_ping.csv
mv 201302/curr_ulping_2013_02.csv ../201302/curr_ulping.csv
mv 201302/curr_dlping_2013_02.csv ../201302/curr_dlping.csv
rm -rf 201302 data-raw-2013-feb.tar.gz
sed -i '1 d' ../201302/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201302/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201302/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-mar.tar.gz > data-raw-2013-mar.tar.gz
tar -xf data-raw-2013-mar.tar.gz
mkdir ../201303
mv 201303/curr_httppostmt_2013_03.csv ../201303/curr_httppostmt.csv
mv 201303/curr_httpgettmt_2013_03.csv ../201303/curr_httpgettmt.csv
mv 201303/curr_udplatency_2013_03.csv ../201303/curr_udplatency.csv
mv 201303/curr_ping_2013_03.csv ../201303/curr_ping.csv
mv 201303/curr_ulping_2013_03.csv ../201303/curr_ulping.csv
mv 201303/curr_dlping_2013_03.csv ../201303/curr_dlping.csv
rm -rf 201303 data-raw-2013-mar.tar.gz
sed -i '1 d' ../201303/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201303/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201303/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-apr.tar.gz > data-raw-2013-apr.tar.gz
tar -xf data-raw-2013-apr.tar.gz
mkdir ../201304
mv 201304/curr_httppostmt_2013_04.csv ../201304/curr_httppostmt.csv
mv 201304/curr_httpgettmt_2013_04.csv ../201304/curr_httpgettmt.csv
mv 201304/curr_udplatency_2013_04.csv ../201304/curr_udplatency.csv
mv 201304/curr_ping_2013_04.csv ../201304/curr_ping.csv
mv 201304/curr_ulping_2013_04.csv ../201304/curr_ulping.csv
mv 201304/curr_dlping_2013_04.csv ../201304/curr_dlping.csv
rm -rf 201304 data-raw-2013-apr.tar.gz
sed -i '1 d' ../201304/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201304/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201304/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-may.tar.gz > data-raw-2013-may.tar.gz
tar -xf data-raw-2013-may.tar.gz
mkdir ../201305
mv 201305/curr_httppostmt_2013_05.csv ../201305/curr_httppostmt.csv
mv 201305/curr_httpgettmt_2013_05.csv ../201305/curr_httpgettmt.csv
mv 201305/curr_udplatency_2013_05.csv ../201305/curr_udplatency.csv
mv 201305/curr_ping_2013_05.csv ../201305/curr_ping.csv
mv 201305/curr_ulping_2013_05.csv ../201305/curr_ulping.csv
mv 201305/curr_dlping_2013_05.csv ../201305/curr_dlping.csv
rm -rf 201305 data-raw-2013-may.tar.gz
sed -i '1 d' ../201305/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201305/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201305/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-june.tar.gz > data-raw-2013-june.tar.gz
tar -xf data-raw-2013-june.tar.gz
mkdir ../201306
mv 201306/curr_httppostmt_2013_06.csv ../201306/curr_httppostmt.csv
mv 201306/curr_httpgettmt_2013_06.csv ../201306/curr_httpgettmt.csv
mv 201306/curr_udplatency_2013_06.csv ../201306/curr_udplatency.csv
mv 201306/curr_ping_2013_06.csv ../201306/curr_ping.csv
mv 201306/curr_ulping_2013_06.csv ../201306/curr_ulping.csv
mv 201306/curr_dlping_2013_06.csv ../201306/curr_dlping.csv
rm -rf 201306 data-raw-2013-june.tar.gz
sed -i '1 d' ../201306/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201306/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201306/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-july.tar.gz > data-raw-2013-july.tar.gz
tar -xf data-raw-2013-july.tar.gz
mkdir ../201307
mv 201307/curr_httppostmt_2013_07.csv ../201307/curr_httppostmt.csv
mv 201307/curr_httpgettmt_2013_07.csv ../201307/curr_httpgettmt.csv
mv 201307/curr_udplatency_2013_07.csv ../201307/curr_udplatency.csv
mv 201307/curr_ping_2013_07.csv ../201307/curr_ping.csv
mv 201307/curr_ulping_2013_07.csv ../201307/curr_ulping.csv
mv 201307/curr_dlping_2013_07.csv ../201307/curr_dlping.csv
rm -rf 201307 data-raw-2013-july.tar.gz
sed -i '1 d' ../201307/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201307/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201307/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-aug.tar.gz > data-raw-2013-aug.tar.gz
tar -xf data-raw-2013-aug.tar.gz
mkdir ../201308
mv 201308/curr_httppostmt_2013_08.csv ../201308/curr_httppostmt.csv
mv 201308/curr_httpgettmt_2013_08.csv ../201308/curr_httpgettmt.csv
mv 201308/curr_udplatency_2013_08.csv ../201308/curr_udplatency.csv
mv 201308/curr_ping_2013_08.csv ../201308/curr_ping.csv
mv 201308/curr_ulping_2013_08.csv ../201308/curr_ulping.csv
mv 201308/curr_dlping_2013_08.csv ../201308/curr_dlping.csv
rm -rf 201308 data-raw-2013-aug.tar.gz
sed -i '1 d' ../201308/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201308/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201308/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-sept.tar.gz > data-raw-2013-sept.tar.gz
tar -xf data-raw-2013-sept.tar.gz
mkdir ../201309
mv 201309/curr_httppostmt_2013_09.csv ../201309/curr_httppostmt.csv
mv 201309/curr_httpgettmt_2013_09.csv ../201309/curr_httpgettmt.csv
mv 201309/curr_udplatency_2013_09.csv ../201309/curr_udplatency.csv
mv 201309/curr_ping_2013_09.csv ../201309/curr_ping.csv
mv 201309/curr_ulping_2013_09.csv ../201309/curr_ulping.csv
mv 201309/curr_dlping_2013_09.csv ../201309/curr_dlping.csv
rm -rf 201309 data-raw-2013-sept.tar.gz
sed -i '1 d' ../201309/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201309/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201309/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-oct.tar.gz > data-raw-2013-oct.tar.gz
tar -xf data-raw-2013-oct.tar.gz
mkdir ../201310
mv 201310/curr_httppostmt_2013_10.csv ../201310/curr_httppostmt.csv
mv 201310/curr_httpgettmt_2013_10.csv ../201310/curr_httpgettmt.csv
mv 201310/curr_udplatency_2013_10.csv ../201310/curr_udplatency.csv
mv 201310/curr_ping_2013_10.csv ../201310/curr_ping.csv
mv 201310/curr_ulping_2013_10.csv ../201310/curr_ulping.csv
mv 201310/curr_dlping_2013_10.csv ../201310/curr_dlping.csv
rm -rf 201310 data-raw-2013-oct.tar.gz
sed -i '1 d' ../201310/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201310/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201310/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-nov.tar.gz > data-raw-2013-nov.tar.gz
tar -xf data-raw-2013-nov.tar.gz
mkdir ../201311
mv 201311/curr_httppostmt_2013_11.csv ../201311/curr_httppostmt.csv
mv 201311/curr_httpgettmt_2013_11.csv ../201311/curr_httpgettmt.csv
mv 201311/curr_udplatency_2013_11.csv ../201311/curr_udplatency.csv
mv 201311/curr_ping_2013_11.csv ../201311/curr_ping.csv
mv 201311/curr_ulping_2013_11.csv ../201311/curr_ulping.csv
mv 201311/curr_dlping_2013_11.csv ../201311/curr_dlping.csv
rm -rf 201311 data-raw-2013-nov.tar.gz
sed -i '1 d' ../201311/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201311/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201311/curr_httppostmt.csv


curl -L https://data.fcc.gov/download/measuring-broadband-america/2014/data-raw-2013-dec.tar.gz > data-raw-2013-dec.tar.gz
tar -xf data-raw-2013-dec.tar.gz
mkdir ../201312
mv 201312/curr_httppostmt_2013_02.csv ../201312/curr_httppostmt.csv
mv 201312/curr_httpgettmt_2013_02.csv ../201312/curr_httpgettmt.csv
mv 201312/curr_udplatency_2013_02.csv ../201312/curr_udplatency.csv
mv 201312/curr_ping_2013_02.csv ../201312/curr_ping.csv
mv 201312/curr_ulping_2013_02.csv ../201312/curr_ulping.csv
mv 201312/curr_dlping_2013_02.csv ../201312/curr_dlping.csv
rm -rf 201312 data-raw-2013-dec.tar.gz
sed -i '1 d' ../201312/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201312/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201312/curr_httppostmt.csv
