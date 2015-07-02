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
print STDERR 'Loading added links';
open(IN, "gunzip -c $DATADIR/links_added_in_02-15.tsv.gz |") or die $!;
while (my $pair = <IN>) {
  chomp $pair;
  $new_links{$pair} = 1;
}
close(IN);

# Load candidate scores.
print STDERR 'Loading candidate scores';
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

$i = 0;
# Load wiki searches before.
print STDERR 'Loading wiki searches before';
open(IN, "gunzip -c $DATADIR/searches_wiki_01-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  last if (++$i == 10000);
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

$i = 0;
# Load wiki searches after.
print STDERR 'Loading wiki searches after';
open(IN, "gunzip -c $DATADIR/searches_wiki_03-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  last if (++$i == 10000);
  chomp $line;
  my ($src, $tgt, $num_searches_302, $num_searches_200) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $wiki_search_counts_302_after{$pair} = $num_searches_302;
    $wiki_search_counts_200_after{$pair} = $num_searches_200;
  }
}
close(IN);

$i = 0;
# Load external searches before.
print STDERR 'Loading external searches before';
open(IN, "gunzip -c $DATADIR/searches_external_01-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  last if (++$i == 10000);
  chomp $line;
  my ($src, $tgt, $num_searches) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $external_search_counts_before{$pair} = $num_searches;
  }
}
close(IN);

$i = 0;
# Load external searches after.
print STDERR 'Loading external searches after';
open(IN, "gunzip -c $DATADIR/searches_external_03-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  last if (++$i == 10000);
  chomp $line;
  my ($src, $tgt, $num_searches) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  # Here we only consider searches corresponding to links that were added.
  if (defined $new_links{$pair}) {
    $external_search_counts_after{$pair} = $num_searches;
  }
}
close(IN);

# Write summary to disk.
print STDERR 'Writing summary to disk';
open(OUT, "| gzip > $DATADIR/links_added_in_02-15_WITH-STATS.tsv.gz");
print OUT join("\t", 'pair', 'num_paths_before', 'num_paths_after', 'num_clicks_before', 'num_clicks_after',
  'num_wiki_searches_302_before', 'num_wiki_searches_200_before', 'num_wiki_searches_302_after',
  'num_wiki_searches_200_after', 'num_external_searches_before', 'num_external_searches_after') . "\n";
foreach my $pair (keys %new_links) {
  $num_paths_before = $path_counts_before{$pair} or 0;
  $num_paths_after = $path_counts_after{$pair} or 0;
  $num_clicks_before = $click_counts_before{$pair} or 0;
  $num_clicks_after = $click_counts_after{$pair} or 0;
  $num_wiki_searches_302_before = $wiki_search_counts_302_before{$pair} or 0;
  $num_wiki_searches_200_before = $wiki_search_counts_200_before{$pair} or 0;
  $num_wiki_searches_302_after = $wiki_search_counts_302_after{$pair} or 0;
  $num_wiki_searches_200_after = $wiki_search_counts_200_after{$pair} or 0;
  $num_external_searches_before = $external_search_counts_before{$pair} or 0;
  $num_external_searches_after = $external_search_counts_after{$pair} or 0;

  print OUT join("\t", $pair, $num_paths_before, $num_paths_after, $num_clicks_before, $num_clicks_after,
    $num_wiki_searches_302_before, $num_wiki_searches_200_before, $num_wiki_searches_302_after,
    $num_wiki_searches_200_after, $num_external_searches_before, $num_external_searches_after) . "\n";
}
close(OUT);
