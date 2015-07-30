#!/usr/bin/perl

my $BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/link_placement/tool/src/main/';

for (my $d = 1; $d <= 30; ++$d) {
  my $date = "year=2015/month=6/day=$d";
  print "============ $date ============\n";
  print `sh $BASEDIR/bash/run_tree_extraction.sh $date 2>&1`;
}

for (my $d = 1; $d <= 30; ++$d) {
  my $date = "year=2015/month=7/day=$d";
  print "============ $date ============\n";
  print `sh $BASEDIR/bash/run_tree_extraction.sh $date 2>&1`;
}
