#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %counts_before = ();
my %counts_after = ();

# Load singleton source counts before.
print STDERR "Loading singleton source counts before\n";
open(IN, "gunzip -c $DATADIR/singleton_counts_01-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  if ($line =~ /^([^ ]+)\t(\d+)$/) {
    my ($src, $count) = ($1, $2);
    $counts_before{$src} += $count;
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
    $counts_after{$src} += $count;
  }
}
close(IN);

# Load relevant data.
print STDERR "Loading relevant pairs\n";
open(IN, "$DATADIR/results/link_addition_effect.tsv") or die $!;
$line = <IN>;
chomp $line;
print "$line\tsource_count_before\tsource_count_after\n";
while (my $line = <IN>) {
  chomp $line;
  my ($src, $_garbage) = split(/\t/, $line, 2);
  print "$line\t$counts_before{$src}\t$counts_after{$src}\n";
  my ($pos, $src, $tgt, $_garbage) = split(/\t/, $line, 4);
  $relevant_pairs{"$src\t$tgt"} = 1;
}
close(IN);
