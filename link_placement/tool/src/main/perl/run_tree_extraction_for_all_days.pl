#!/usr/bin/perl

my $BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/link_placement/tool/src/main/';

for (my $d = 1; $d <= 30; ++$d) {
  print `sh $BASEDIR/bash/run_tree_extraction.sh year=2015/month=6/day=$d`;
}

for (my $d = 1; $d <= 30; ++$d) {
  print `sh $BASEDIR/bash/run_tree_extraction.sh year=2015/month=7/day=$d`;
}
