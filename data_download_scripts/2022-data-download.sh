#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-jan.tar.gz > data-raw-2022-jan.tar.gz
tar -xf data-raw-2022-jan.tar.gz
cd 202201 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-jan.tar.gz
mv 202201 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-feb.tar.gz > data-raw-2022-feb.tar.gz
tar -xf data-raw-2022-feb.tar.gz
cd 202202 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-feb.tar.gz
mv 202202 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-mar.tar.gz > data-raw-2022-mar.tar.gz
tar -xf data-raw-2022-mar.tar.gz
cd 202203 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-mar.tar.gz
mv 202203 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-apr.tar.gz > data-raw-2022-apr.tar.gz
tar -xf data-raw-2022-apr.tar.gz
cd 202204 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-apr.tar.gz
mv 202204 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-may.tar.gz > data-raw-2022-may.tar.gz
tar -xf data-raw-2022-may.tar.gz
cd 202205 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-may.tar.gz
mv 202205 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-jun.tar.gz > data-raw-2022-jun.tar.gz
tar -xf data-raw-2022-jun.tar.gz
cd 202206 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-jun.tar.gz
mv 202206 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-jul.tar.gz > data-raw-2022-jul.tar.gz
tar -xf data-raw-2022-jul.tar.gz
cd 202207 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-jul.tar.gz
mv 202207 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-aug.tar.gz > data-raw-2022-aug.tar.gz
tar -xf data-raw-2022-aug.tar.gz
cd 202208 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-aug.tar.gz
mv 202208 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-nov.tar.gz > data-raw-2022-nov.tar.gz
tar -xf data-raw-2022-nov.tar.gz
cd 202211 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-nov.tar.gz
mv 202211 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2022/data-raw-2022-dec.tar.gz > data-raw-2022-dec.tar.gz
tar -xf data-raw-2022-dec.tar.gz
cd 202212 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv
cd .. && rm data-raw-2022-dec.tar.gz
mv 202212 ../.