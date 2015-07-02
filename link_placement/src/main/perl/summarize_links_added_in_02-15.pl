#!/usr/bin/perl

use Digest::MD5 qw(md5 md5_hex md5_base64);

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %new_links = ();
my %path_counts_before = ();
my %path_counts_after = ();
my %click_counts_before = ();
my %click_counts_after = ();
my %wiki_search_counts_200_before = ();
my %wiki_search_counts_200_after = ();
my %wiki_search_counts_302_before = ();
my %wiki_search_counts_302_after = ();
my %external_search_counts_before = ();
my %external_search_counts_after = ();

# Load added links.
open(IN, "gunzip -c $DATADIR/links_added_in_02-15.tsv.gz |") or die $!;
while (my $pair = <IN>) {
  chomp $pair;
  $new_links{$pair} = 1;
}
close(IN);

# Load candidate scores.
open(IN, "gunzip -c $DATADIR/link_candidates_scores_GROUND-TRUTH.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $prob_direct_after, $num_paths_after, $num_paths_before, $num_clicks_before)
    = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # If there were direct clicks before the link was added according to the edit history, this is
  # due to wiki searches or because the link existed, but was hidden in a template.
  $path_counts_before{$pair} = $num_paths_before;
  $path_counts_after{$pair} = $num_paths_after;
  $click_counts_before{$pair} = $num_clicks_before;
  $click_counts_after{$pair} = sprintf("%.0f", $prob_direct_after * $num_paths_after);
}
close(IN);

# Load wiki searches before.
open(IN, "gunzip -c $DATADIR/searches_wiki_01-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $num_searches_302, $num_searches_200) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $wiki_search_counts_302_before{$pair} = $num_searches_302;
    $wiki_search_counts_200_before{$pair} = $num_searches_200;
  }
}
close(IN);

# Load wiki searches after.
open(IN, "gunzip -c $DATADIR/searches_wiki_03-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $num_searches_302, $num_searches_200) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $wiki_search_counts_302_after{$pair} = $num_searches_302;
    $wiki_search_counts_200_after{$pair} = $num_searches_200;
print "$pair\t$wiki_search_counts_302_after{$pair}\n";
  }
}
close(IN);

# Load external searches before.
open(IN, "gunzip -c $DATADIR/searches_external_01-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $num_searches) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $external_search_counts_before{$pair} = $num_searches;
  }
}
close(IN);

# Load external searches after.
open(IN, "gunzip -c $DATADIR/searches_external_03-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $num_searches) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $external_search_counts_after{$pair} = $num_searches;
  }
}
close(IN);
