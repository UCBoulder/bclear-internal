#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-jan.tar.gz > data-raw-2019-jan.tar.gz
tar -xf data-raw-2019-jan.tar.gz
cd 201901 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-jan.tar.gz
mv 201901 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-feb.tar.gz > data-raw-2019-feb.tar.gz
tar -xf data-raw-2019-feb.tar.gz
cd 201902 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-feb.tar.gz
mv 201902 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-mar.tar.gz > data-raw-2019-mar.tar.gz
tar -xf data-raw-2019-mar.tar.gz
cd 201903 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-mar.tar.gz
mv 201903 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-apr.tar.gz > data-raw-2019-apr.tar.gz
tar -xf data-raw-2019-apr.tar.gz
cd 201904 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-apr.tar.gz
mv 201904 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-may.tar.gz > data-raw-2019-may.tar.gz
tar -xf data-raw-2019-may.tar.gz
cd 201905 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-may.tar.gz
mv 201905 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-jun.tar.gz > data-raw-2019-jun.tar.gz
tar -xf data-raw-2019-jun.tar.gz
cd 201906 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-jun.tar.gz
mv 201906 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-jul.tar.gz > data-raw-2019-jul.tar.gz
tar -xf data-raw-2019-jul.tar.gz
cd 201907 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-jul.tar.gz
mv 201907 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-aug.tar.gz > data-raw-2019-aug.tar.gz
tar -xf data-raw-2019-aug.tar.gz
cd 201908 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-aug.tar.gz
mv 201908 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-sept.tar.gz > data-raw-2019-sept.tar.gz
tar -xf data-raw-2019-sept.tar.gz
cd 201909 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-sept.tar.gz
mv 201909 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-oct.tar.gz > data-raw-2019-oct.tar.gz
tar -xf data-raw-2019-oct.tar.gz
cd 201910 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-oct.tar.gz
mv 201910 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-nov.tar.gz > data-raw-2019-nov.tar.gz
tar -xf data-raw-2019-nov.tar.gz
cd 201911 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-nov.tar.gz
mv 201911 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2019/data-raw-2019-dec.tar.gz > data-raw-2019-dec.tar.gz
tar -xf data-raw-2019-dec.tar.gz
cd 201912 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2019-dec.tar.gz
mv 201912 ../.
