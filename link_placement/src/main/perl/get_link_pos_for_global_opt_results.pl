#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %relevant_pairs = ();

# Load singleton source counts before.
print STDERR "Loading singleton source counts before\n";
open(IN, "gunzip -c $DATADIR/singleton_counts_01-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  if ($line =~ /^([^ ]+)\t(\d+)$/) {
    my ($src, $count) = ($1, $2);
    $source_counts_before{$src} += $count if (defined $relevant_sources{$src});
  }
}
close(IN);

# Load singleton source counts after.
print STDERR "Loading singleton source counts after\n";
open(IN, "gunzip -c $DATADIR/singleton_counts_03-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  if ($line =~ /^([^ ]+)\t(\d+)$/) {
    my ($src, $count) = ($1, $2);
    # Because of some data issue some sources appear twice; add these counts.
    $source_counts_after{$src} += $count if (defined $relevant_sources{$src});
  }
}
close(IN);

# Load relevant data.
print STDERR "Loading relevant pairs\n";
open(IN, "gunzip -c $DATADIR/results/link_placement_results_$objective.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($pos, $src, $tgt, $_garbage) = split(/\t/, $line, 4);
  $relevant_pairs{"$src\t$tgt"} = 1;
}
close(IN);
