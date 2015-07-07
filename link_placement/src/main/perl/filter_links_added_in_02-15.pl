#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %new_links = ();
my %existing_links = ();
my %existing_pages = ();

# Load link positions.
print STDERR "Loading link positions\n";
open(IN, "gunzip -c $DATADIR/link_positions_added_in_02-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $size, $pos) = split(/\t/, $line);
  $existing_links{"$src\t$tgt"} = 1;
  $existing_pages{$src} = 1;
}
close(IN);

# Filter links, keeping only those that didn't exist before. Additionally, the source must have
# existed before.
print STDERR "Filtering links\n";
open(OUT, "| gzip > $DATADIR/links_added_in_02-15_FILTERED.tsv.gz") or die $!;
open(IN, "gunzip -c $DATADIR/links_added_in_02-15.tsv.gz |") or die $!;
while (my $pair = <IN>) {
  chomp $pair;
  my ($src, $tgt) = split(/\t/, $pair);
  if ($existing_pages{$src} && !$existing_links{$pair}) {
    print OUT "$pair\n";
  }
}
close(OUT);
close(IN);
