#!/usr/local/bin/bash

#this is a config file with paths of various directories and code that
#is needed for the probing update process 
#you will need to change each of these to point to the right locations
#on your system

#location of code
codedir="/home/amogh/projects/comcast_ping/code/tslp-public/probing-update"

#location of files with csum for each monitor. filename is of format
#MON.csum.txt and simly contains the csum value to use for this monitor
csumdir="/project/comcast-ping/TSP/adaptive/csums"

#location of files with sibling lists for each monitor. filename is of
#format MON.sibling.txt. it contains a list of siblings, one per line
sibdir="/project/comcast-ping/TSP/adaptive/siblings"

#location of prefix2AS and as-rel files
pfx2asdir="/project/comcast-ping/TSP/adaptive/pfx2AS"

#location of delegated files
delegateddir="/project/comcast-ping/TSP/adaptive/pfx2AS/delegated"

#location of the bdrmap-state directory corresponding to each
#monitor. this is the base directory for all monitors
mondatadirbase="/project/comcast-ping/TSP/devel/update/mon_data_lnk"

#the probing update process using the sqlite DB runs faster if we use a memory FS
#this is the location of memory fs
memfsdir="/mfs/amogh"

#path of the sc_bdrmap binary
bdrmap="/home/amogh/projects/comcast_ping/code/bdrmap/bdrmap-CVS-20170602/sc_bdrmap"

#filenames of current delegated, pfx2as, peering, and asrelationship files
delegatedfile="201807/delegated-ipv4-201807.txt";
pfx2asfile="20180701.prefix2as";
peering="20160321.peering";
asrel="20180701.as-rel.txt";
