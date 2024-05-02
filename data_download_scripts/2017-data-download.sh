#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-jan.tar.gz > data-raw-2017-jan.tar.gz
mkdir 201701 && cd 201701
tar -xf ../data-raw-2017-jan.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-jan.tar.gz
mv 201701 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-feb.tar.gz > data-raw-2017-feb.tar.gz
mkdir 201702 && cd 201702
tar -xf ../data-raw-2017-feb.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-feb.tar.gz
mv 201702 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-mar.tar.gz > data-raw-2017-mar.tar.gz
mkdir 201703 && cd 201703
tar -xf ../data-raw-2017-mar.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-mar.tar.gz
mv 201703 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-apr.tar.gz > data-raw-2017-apr.tar.gz
mkdir 201704 && cd 201704
tar -xf ../data-raw-2017-apr.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-apr.tar.gz
mv 201704 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-may.tar.gz > data-raw-2017-may.tar.gz
mkdir 201705 && cd 201705
tar -xf ../data-raw-2017-may.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-may.tar.gz
mv 201705 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-jun.tar.gz > data-raw-2017-jun.tar.gz
mkdir 201706 && cd 201706
tar -xf ../data-raw-2017-jun.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-jun.tar.gz
mv 201706 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-jul.tar.gz > data-raw-2017-jul.tar.gz
mkdir 201707 && cd 201707
tar -xf ../data-raw-2017-jul.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-jul.tar.gz
mv 201707 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-aug.tar.gz > data-raw-2017-aug.tar.gz
mkdir 201708 && cd 201708
tar -xf ../data-raw-2017-aug.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-aug.tar.gz
mv 201708 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-sept.tar.gz > data-raw-2017-sept.tar.gz
mkdir 201709 && cd 201709
tar -xf ../data-raw-2017-sept.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-sept.tar.gz
mv 201709 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-oct.tar.gz > data-raw-2017-oct.tar.gz
mkdir 201710 && cd 201710
tar -xf ../data-raw-2017-oct.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-oct.tar.gz
mv 201710 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-nov.tar.gz > data-raw-2017-nov.tar.gz
mkdir 201711 && cd 201711
tar -xf ../data-raw-2017-nov.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-nov.tar.gz
mv 201711 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2017/data-raw-2017-dec.tar.gz > data-raw-2017-dec.tar.gz
mkdir 201712 && cd 201712
tar -xf ../data-raw-2017-dec.tar.gz
rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httpgetmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv curr_lct_dl_intermediate.csv curr_lct_ul_intermediate.csv curr_netusage.csv curr_videostream.csv curr_udplatency.csv curr_ping.csv curr_dlping.csv curr_ulping.csv 2>&1
cd .. && rm data-raw-2017-dec.tar.gz
mv 201712 ../.