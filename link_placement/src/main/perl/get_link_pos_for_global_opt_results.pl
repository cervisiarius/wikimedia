#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %relevant_pairs = ();

# Load results.
foreach my $objective ('CHAIN', 'TREE', 'COINS') {
  print STDERR "Loading results from $objective model\n";
  open(IN, "gunzip -c $DATADIR/results/link_placement_results_$objective.tsv.gz |") or die $!;
  while (my $line = <IN>) {
    chomp $line;
    my ($pos, $src, $tgt, $_garbage) = split(/\t/, $line, 4);
    $relevant_pairs{"$src\t$tgt"} = 1;
  }
  close(IN);
}

print STDERR "Streaming over link positions in wikidump of 2015-03-04\n";
open(IN, "gunzip -c $DATADIR/wikipedia_link_positions_enwiki-20150304.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $length, $pos) = split(/\t/, $line, 4);
  my @pos_array = split(/,/, $pos);
  my $min_pos = $pos_array[0];
  my $pair = "$src\t$tgt";
  print "$pair\t$length\t$min_pos\n" if ($relevant_pairs{$pair});
}
close(IN);
