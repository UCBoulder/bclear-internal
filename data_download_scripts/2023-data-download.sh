#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-jan.tar.gz > data-raw-2023-jan.tar.gz
tar -xf data-raw-2023-jan.tar.gz 
cd 202301 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2023-jan.tar.gz
mv 202301 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-feb.tar.gz > data-raw-2023-feb.tar.gz
tar -xf data-raw-2023-feb.tar.gz
cd 202302 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2023-feb.tar.gz
mv 202302 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-mar.tar.gz > data-raw-2023-mar.tar.gz
tar -xf data-raw-2023-mar.tar.gz
# cd 202303 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
# cd .. && rm data-raw-2023-mar.tar.gz
# mv 202303 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-apr.tar.gz > data-raw-2023-apr.tar.gz
tar -xf data-raw-2023-apr.tar.gz
cd 202304 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2023-apr.tar.gz
mv 202304 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-may.tar.gz > data-raw-2023-may.tar.gz
tar -xf data-raw-2023-may.tar.gz
cd 202305 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2023-may.tar.gz
mv 202305 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-jun.tar.gz > data-raw-2023-jun.tar.gz
tar -xf data-raw-2023-jun.tar.gz
cd 202306 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2023-jun.tar.gz
mv 202306 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2023/data-raw-2023-jul.tar.gz > data-raw-2023-jul.tar.gz
tar -xf data-raw-2023-jul.tar.gz
cd 202307 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2023-jul.tar.gz
mv 202307 ../.