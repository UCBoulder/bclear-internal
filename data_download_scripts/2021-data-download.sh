#!/bin/bash

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-jan.tar.gz > data-raw-2021-jan.tar.gz
tar -xf data-raw-2021-jan.tar.gz
cd 202101 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-jan.tar.gz 
mv 202101 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-feb.tar.gz > data-raw-2021-feb.tar.gz
tar -xf data-raw-2021-feb.tar.gz
cd 202102 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-feb.tar.gz 
mv 202102 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-mar.tar.gz > data-raw-2021-mar.tar.gz
tar -xf data-raw-2021-mar.tar.gz
cd 202103 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-mar.tar.gz 
mv 202103 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-apr.tar.gz > data-raw-2021-apr.tar.gz
tar -xf data-raw-2021-apr.tar.gz
cd 202104 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-apr.tar.gz 
mv 202104 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-may.tar.gz > data-raw-2021-may.tar.gz
tar -xf data-raw-2021-may.tar.gz
cd 202105 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-may.tar.gz 
mv 202105 ../.

curl -L http://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-jun.tar.gz > data-raw-2021-jun.tar.gz
tar -xf data-raw-2021-jun.tar.gz
cd 202106 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-jun.tar.gz 
mv 202106 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-jul.tar.gz > data-raw-2021-jul.tar.gz
tar -xf data-raw-2021-jul.tar.gz
cd 202107 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-jul.tar.gz 
mv 202107 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-aug.tar.gz > data-raw-2021-aug.tar.gz
tar -xf data-raw-2021-aug.tar.gz
cd 202108 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
rm data-raw-2021-aug.tar.gz
mv 202108 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-sep.tar.gz > data-raw-2021-sep.tar.gz
tar -xf data-raw-2021-sep.tar.gz
cd 202109 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
cd .. && rm data-raw-2021-sep.tar.gz
mv 202109 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-oct.tar.gz > data-raw-2021-oct.tar.gz
tar -xf data-raw-2021-oct.tar.gz
cd 202110 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
cd .. && rm data-raw-2021-oct.tar.gz
mv 202110 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-nov.tar.gz > data-raw-2021-nov.tar.gz
tar -xf data-raw-2021-nov.tar.gz
cd 202111 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
cd .. && rm data-raw-2021-nov.tar.gz
mv 202111 ../.

curl -L https://data.fcc.gov/download/measuring-broadband-america/2021/data-raw-2021-dec.tar.gz > data-raw-2021-dec.tar.gz
tar -xf data-raw-2021-dec.tar.gz
cd 202112 && rm curr_datausage.csv curr_dns.csv curr_httpget.csv curr_httpgetmt6.csv curr_httppost.csv curr_httppostmt.csv curr_httppostmt6.csv curr_lct_dl.csv curr_lct_ul.csv curr_traceroute.csv curr_udpcloss.csv curr_udpjitter.csv curr_udplatency6.csv curr_webget.csv
cd .. && rm data-raw-2021-dec.tar.gz
mv 202112 ../.