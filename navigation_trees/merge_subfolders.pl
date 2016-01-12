#!/usr/bin/perl

for (my $i = 0; $i < 100; ++$i) {
    my $s = $i < 10 ? "0000$i" : "000$i";
    print "hadoop fs -mv navigation_trees/year=2015/month=11/en/before_2015-11-10/part-r-$s navigation_trees/year=2015/month=11/en/part-r-$s\_a\n";
    print "hadoop fs -mv navigation_trees/year=2015/month=11/en/after_2015-11-10/part-r-$s navigation_trees/year=2015/month=11/en/part-r-$s\_b\n";
    `hadoop fs -mv navigation_trees/year=2015/month=11/en/before_2015-11-10/part-r-$s navigation_trees/year=2015/month=11/en/part-r-$s\_a`;
    `hadoop fs -mv navigation_trees/year=2015/month=11/en/after_2015-11-10/part-r-$s navigation_trees/year=2015/month=11/en/part-r-$s\_b`;
}
