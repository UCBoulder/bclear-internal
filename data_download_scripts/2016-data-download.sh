#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-jan.tar.gz > data-raw-2016-jan.tar.gz
mkdir -p ../201601
tar -xf data-raw-2016-jan.tar.gz
mv 201601/curr_httppostmt_2016_01.csv ../201601/curr_httppostmt.csv
mv 201601/curr_httpgetmt_2016_01.csv ../201601/curr_httpgetmt.csv
mv 201601/curr_udplatency_2016_01.csv ../201601/curr_udplatency.csv
mv 201601/curr_ping_2016_01.csv ../201601/curr_ping.csv
mv 201601/curr_dlping_2016_01.csv ../201601/curr_dlping.csv
mv 201601/curr_ulping_2016_01.csv ../201601/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201601/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201601/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201601/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201601/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201601/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201601/curr_udplatency.csv
rm -rf 201601 data-raw-2016-jan.tar.gz


curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-feb.tar.gz > data-raw-2016-feb.tar.gz
mkdir -p ../201602
tar -xf data-raw-2016-feb.tar.gz
mv 201602/curr_httppostmt_2016_02.csv ../201602/curr_httppostmt.csv
mv 201602/curr_httpgetmt_2016_02.csv ../201602/curr_httpgetmt.csv
mv 201602/curr_udplatency_2016_02.csv ../201602/curr_udplatency.csv
mv 201602/curr_ping_2016_02.csv ../201602/curr_ping.csv
mv 201602/curr_dlping_2016_02.csv ../201602/curr_dlping.csv
mv 201602/curr_ulping_2016_02.csv ../201602/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201602/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201602/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201602/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201602/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201602/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201602/curr_udplatency.csv
rm -rf 201602 data-raw-2016-feb.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-mar.tar.gz > data-raw-2016-mar.tar.gz
mkdir -p ../201603
tar -xf data-raw-2016-mar.tar.gz
mv 201603/curr_httppostmt_2016_03.csv ../201603/curr_httppostmt.csv
mv 201603/curr_httpgetmt_2016_03.csv ../201603/curr_httpgetmt.csv
mv 201603/curr_udplatency_2016_03.csv ../201603/curr_udplatency.csv
mv 201603/curr_ping_2016_03.csv ../201603/curr_ping.csv
mv 201603/curr_dlping_2016_03.csv ../201603/curr_dlping.csv
mv 201603/curr_ulping_2016_03.csv ../201603/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201603/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201603/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201603/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201603/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201603/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201603/curr_udplatency.csv
rm -rf 201603 data-raw-2016-mar.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-apr.tar.gz > data-raw-2016-apr.tar.gz
mkdir -p ../201604
tar -xf data-raw-2016-apr.tar.gz
mv 201604/curr_httppostmt_2016_04.csv ../201604/curr_httppostmt.csv
mv 201604/curr_httpgetmt_2016_04.csv ../201604/curr_httpgetmt.csv
mv 201604/curr_udplatency_2016_04.csv ../201604/curr_udplatency.csv
mv 201604/curr_ping_2016_04.csv ../201604/curr_ping.csv
mv 201604/curr_dlping_2016_04.csv ../201604/curr_dlping.csv
mv 201604/curr_ulping_2016_04.csv ../201604/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201604/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201604/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201604/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201604/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201604/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201604/curr_udplatency.csv
rm -rf 201604 data-raw-2016-apr.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-may.tar.gz > data-raw-2016-may.tar.gz
tar -xf data-raw-2016-may.tar.gz
mkdir -p ../201605
mv 201605/curr_httppostmt_2016_05.csv ../201605/curr_httppostmt.csv
mv 201605/curr_httpgetmt_2016_05.csv ../201605/curr_httpgetmt.csv
mv 201605/curr_udplatency_2016_05.csv ../201605/curr_udplatency.csv
mv 201605/curr_ping_2016_05.csv ../201605/curr_ping.csv
mv 201605/curr_dlping_2016_05.csv ../201605/curr_dlping.csv
mv 201605/curr_ulping_2016_05.csv ../201605/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201605/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201605/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201605/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201605/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201605/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201605/curr_udplatency.csv
rm -rf 201605 data-raw-2016-may.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-jun.tar.gz > data-raw-2016-jun.tar.gz
tar -xf data-raw-2016-jun.tar.gz
mkdir -p ../201606
mv 201606/curr_httppostmt_2016_06.csv ../201606/curr_httppostmt.csv
mv 201606/curr_httpgetmt_2016_06.csv ../201606/curr_httpgetmt.csv
mv 201606/curr_udplatency_2016_06.csv ../201606/curr_udplatency.csv
mv 201606/curr_ping_2016_06.csv ../201606/curr_ping.csv
mv 201606/curr_dlping_2016_06.csv ../201606/curr_dlping.csv
mv 201606/curr_ulping_2016_06.csv ../201606/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201606/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201606/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201606/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201606/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201606/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201606/curr_udplatency.csv
rm -rf 201606 data-raw-2016-jun.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-jul.tar.gz > data-raw-2016-jul.tar.gz
tar -xf data-raw-2016-jul.tar.gz
mkdir -p ../201607
mv 201607/curr_httppostmt_2016_07.csv ../201607/curr_httppostmt.csv
mv 201607/curr_httpgetmt_2016_07.csv ../201607/curr_httpgetmt.csv
mv 201607/curr_udplatency_2016_07.csv ../201607/curr_udplatency.csv
mv 201607/curr_ping_2016_07.csv ../201607/curr_ping.csv
mv 201607/curr_dlping_2016_07.csv ../201607/curr_dlping.csv
mv 201607/curr_ulping_2016_07.csv ../201607/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201607/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201607/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201607/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201607/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201607/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201607/curr_udplatency.csv
rm -rf 201607 data-raw-2016-jul.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-aug.tar.gz > data-raw-2016-aug.tar.gz
tar -xf data-raw-2016-aug.tar.gz
mkdir -p ../201608
mv 201608/curr_httppostmt_2016_08.csv ../201608/curr_httppostmt.csv
mv 201608/curr_httpgetmt_2016_08.csv ../201608/curr_httpgetmt.csv
mv 201608/curr_udplatency_2016_08.csv ../201608/curr_udplatency.csv
mv 201608/curr_ping_2016_08.csv ../201608/curr_ping.csv
mv 201608/curr_dlping_2016_08.csv ../201608/curr_dlping.csv
mv 201608/curr_ulping_2016_08.csv ../201608/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201608/curr_httpgetmt.csv
sed -i 1i'unit_id,dtime,target,address,fetch_time,bytes_total,bytes_sec,bytes_sec_interval,warmup_time,warmup_bytes,sequence,threads,successes,failures,location_id' ../201608/curr_httppostmt.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201608/curr_ping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201608/curr_dlping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201608/curr_ulping.csv
sed -i 1i'unit_id,dtime,target,rtt_avg,rtt_min,rtt_max,rtt_std,successes,failures,location_id' ../201608/curr_udplatency.csv
rm -rf 201608 data-raw-2016-aug.tar.gz

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-sept.tar.gz > data-raw-2016-sept.tar.gz
mkdir 201609
tar -xf data-raw-2016-sept.tar.gz -C 201609/.
cd 201609 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv 2>&1
cd .. && rm data-raw-2016-sept.tar.gz
mv 201609 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-oct.tar.gz > data-raw-2016-oct.tar.gz
mkdir 201610
tar -xf data-raw-2016-oct.tar.gz -C 201610/.
cd 201610 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv 2>&1
cd .. && rm data-raw-2016-oct.tar.gz
mv 201610 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-nov.tar.gz > data-raw-2016-nov.tar.gz
mkdir 201611
tar -xf data-raw-2016-nov.tar.gz -C 201611/.
cd 201611 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv 2>&1
cd .. && rm data-raw-2016-nov.tar.gz
mv 201611 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2016-dec.tar.gz > data-raw-2016-dec.tar.gz
mkdir 201612
tar -xf data-raw-2016-dec.tar.gz -C 201612/.
cd 201612 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv 2>&1
cd .. && rm data-raw-2016-dec.tar.gz
mv 201612 ../.