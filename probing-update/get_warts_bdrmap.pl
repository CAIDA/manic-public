#!/usr/bin/perl
#
# $Id: get_warts_bdrmap.pl,v 1.3 2016/02/19 22:17:33 amogh Exp $

use strict;

if(scalar(@ARGV) != 2)
{
    print STDERR "usage: get_warts_bdrmap.pl \$dir\n";
    exit -1;
}

my $dir = $ARGV[0];
my $min_ts = $ARGV[1];

my %flist;
opendir(DIR,$dir);
while (my $fn = readdir (DIR))
  {
    next if $fn =~ m/^\./;
    next if $fn !~ m/warts\.gz$/;
    
    my $ts = (split /\./,$fn)[-4];
    $flist{$ts} = $fn;
  }

foreach (sort {$b <=> $a} keys %flist)
#TEMPORARY!
#foreach (sort {$a <=> $b} keys %flist)
  {
    if ($_ > $min_ts)
      {
	print $dir."/".$flist{$_}."\n";
	last;
      }
  }
