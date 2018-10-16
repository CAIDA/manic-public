#!/usr/bin/perl

use strict;
use warnings;

my $datadir= "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/ddc/";
my $csvdata = $datadir."*.csv";
my @filelist = `ls -1 $csvdata`;
chomp @filelist;

my $asn=$ARGV[0];

for my $f (@filelist){
    #    my $fullpath = $datadir.$f;
    #print $fullpath."\n";
    my $cmd = "cat ".$f."| grep ,".$asn;
    system($cmd);
    #    chomp @googleline;

}

