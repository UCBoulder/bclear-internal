#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-jan.tar.gz > data-raw-2020-jan.tar.gz
tar -xf data-raw-2020-jan.tar.gz
cd 202001 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-jan.tar.gz
mv 202001 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-feb.tar.gz > data-raw-2020-feb.tar.gz
tar -xf data-raw-2020-feb.tar.gz
cd 202002 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-feb.tar.gz
mv 202002 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-mar.tar.gz > data-raw-2020-mar.tar.gz
tar -xf data-raw-2020-mar.tar.gz
cd 202003 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-mar.tar.gz
mv 202003 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-apr.tar.gz > data-raw-2020-apr.tar.gz
tar -xf data-raw-2020-apr.tar.gz
cd 202004 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-apr.tar.gz
mv 202004 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-may.tar.gz > data-raw-2020-may.tar.gz
tar -xf data-raw-2020-may.tar.gz
cd 202005 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-may.tar.gz
mv 202005 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-jun.tar.gz > data-raw-2020-jun.tar.gz
tar -xf data-raw-2020-jun.tar.gz
cd 202006 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-jun.tar.gz
mv 202006 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-jul.tar.gz > data-raw-2020-jul.tar.gz
tar -xf data-raw-2020-jul.tar.gz
cd 202007 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-jul.tar.gz
mv 202007 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-aug.tar.gz > data-raw-2020-aug.tar.gz
tar -xf data-raw-2020-aug.tar.gz
cd 202008 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-aug.tar.gz
mv 202008 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-nov.tar.gz > data-raw-2020-nov.tar.gz
tar -xf data-raw-2020-nov.tar.gz
cd 202011 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-nov.tar.gz
mv 202011 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-dec.tar.gz > data-raw-2020-dec.tar.gz
tar -xf data-raw-2020-dec.tar.gz
cd 202012 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2020-dec.tar.gz
mv 202012 ../.