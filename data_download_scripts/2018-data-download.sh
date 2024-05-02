#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-jan.tar.gz > data-raw-2018-jan.tar.gz
tar -xf data-raw-2018-jan.tar.gz
cd 201801 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-jan.tar.gz
mv 201801 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-feb.tar.gz > data-raw-2018-feb.tar.gz
tar -xf data-raw-2018-feb.tar.gz
cd 201802 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-feb.tar.gz
mv 201802 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-mar.tar.gz > data-raw-2018-mar.tar.gz
tar -xf data-raw-2018-mar.tar.gz
cd 201803 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-mar.tar.gz
mv 201803 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-apr.tar.gz > data-raw-2018-apr.tar.gz
tar -xf data-raw-2018-apr.tar.gz
cd 201804 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-apr.tar.gz
mv 201804 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-may.tar.gz > data-raw-2018-may.tar.gz
tar -xf data-raw-2018-may.tar.gz
cd 201805 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-may.tar.gz
mv 201805 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-jun.tar.gz > data-raw-2018-jun.tar.gz
tar -xf data-raw-2018-jun.tar.gz
cd 201806 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-jun.tar.gz
mv 201806 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-jul.tar.gz > data-raw-2018-jul.tar.gz
tar -xf data-raw-2018-jul.tar.gz
cd 201807 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-jul.tar.gz
mv 201807 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-aug.tar.gz > data-raw-2018-aug.tar.gz
tar -xf data-raw-2018-aug.tar.gz
cd 201808 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-aug.tar.gz
mv 201808 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-sept.tar.gz > data-raw-2018-sept.tar.gz
tar -xf data-raw-2018-sept.tar.gz
cd 201809 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-sept.tar.gz
mv 201809 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-oct.tar.gz > data-raw-2018-oct.tar.gz
tar -xf data-raw-2018-oct.tar.gz
cd 201810 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-oct.tar.gz
mv 201810 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-nov.tar.gz > data-raw-2018-nov.tar.gz
tar -xf data-raw-2018-nov.tar.gz
cd 201811 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-nov.tar.gz
mv 201811 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2018/data-raw-2018-dec.tar.gz > data-raw-2018-dec.tar.gz
tar -xf data-raw-2018-dec.tar.gz
cd 201812 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2018-dec.tar.gz
mv 201812 ../.
