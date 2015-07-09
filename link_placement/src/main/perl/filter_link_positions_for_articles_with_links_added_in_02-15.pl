#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %new_links = ();

# Load added links.
print STDERR "Loading added links\n";
open(IN, "gunzip -c $DATADIR/links_added_in_02-15_FILTERED.tsv.gz |") or die $!;
while (my $pair = <IN>) {
  chomp $pair;
  my ($src, $tgt) = split(/\t/, $pair);
  $new_links{$pair} = 1;
}
close(IN);

# Stream over positions.
print STDERR "Streaming over link positions\n";
open(OUT, "| gzip > $DATADIR/link_positions_enwiki-20150304_FILTERED.tsv.gz") or die $!;
open(IN, "gunzip -c $DATADIR/wikipedia_link_positions_enwiki-20150304.tsv.gz |") or die $!;
while (my $line = <IN>) {
  my ($src, $tgt, $size, $pos) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  print OUT $line if (defined $new_links{$pair});
}
close(IN);
close(OUT);
