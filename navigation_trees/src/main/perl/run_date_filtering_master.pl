#!/usr/bin/perl

my @langs = split(/\|/, 'sh|pt|ar|nl|it|ceb|war|sr|pl|uk|ca|id|ro|tr|ko|no|fi|uz|cs|hu|vi|he|hy|eo|da|bg|et|lt|el|vo|sk|sl|eu|nn|kk|hr|hi|ms|gl|min');
my $NUM_REDUCERS = 50;
my $HOME = $ENV{'HOME'};

foreach my $lang (@langs) {
  print "Processing $lang\n";
  `sh $HOME/wikimedia/trunk/navigation_trees/run_date_filtering.sh $lang $NUM_REDUCERS`;
}

