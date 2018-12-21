#!/usr/bin/perl
#this script is used to grep the time period of congestion of a certain AS indicated by autocorrelation
#usage: perl validsample.pl <ASN>
#Require access to MANIC server. beamer.caida.org:/project/comcast-ping/kabir-plots/loss_data/supplemental_data/ddc/

use strict;
use warnings;

my $datadir= "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/ddc/";
my $csvdata = $datadir."*.csv";
my @filelist = `ls -1 $csvdata`;
chomp @filelist;

my $asn=$ARGV[0];

for my $f (@filelist){
    my $cmd = "cat ".$f."| grep ,".$asn;
    system($cmd);
}

