#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";

my %relevant_sources = ();
my %new_links = ();
my %source_counts_before = ();
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
print STDERR "Loading added links\n";
open(IN, "gunzip -c $DATADIR/links_added_in_02-15_FILTERED.tsv.gz |") or die $!;
while (my $pair = <IN>) {
  chomp $pair;
  my ($src, $tgt) = split(/\t/, $pair);
  $new_links{$pair} = 1;
  $relevant_sources{$src} = 1;
}
close(IN);

# # Load link positions.
# print STDERR "Loading link positions\n";
# open(IN, "gunzip -c $DATADIR/link_positions_added_in_02-15.tsv.gz |") or die $!;
# while (my $line = <IN>) {
#   chomp $line;
#   my ($src, $tgt, $size, $pos) = split(/\t/, $line);
#   $existing_links{"$src\t$tgt"} = 1;
#   $existing_pages{$src} = 1;
# }
# close(IN);

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

# Load links with at least one path before.
print STDERR "Loading links with path before\n";
open(IN, "gunzip -c $DATADIR/path_summaries_01-15_added_in_02-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $num_paths_before, $num_clicks_before) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  $path_counts_before{$pair} = $num_paths_before;
  $click_counts_before{$pair} = $num_clicks_before;
}
close(IN);

# Load links with at least one path after.
print STDERR "Loading links with path after\n";
open(IN, "gunzip -c $DATADIR/path_summaries_03-15_added_in_02-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
  chomp $line;
  my ($src, $tgt, $num_paths_after, $num_clicks_after) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  $path_counts_after{$pair} = $num_paths_after;
  $click_counts_after{$pair} = $num_clicks_after;
}
close(IN);

# Load wiki searches before.
print STDERR "Loading wiki searches before\n";
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
print STDERR "Loading wiki searches after\n";
open(IN, "gunzip -c $DATADIR/searches_wiki_03-15.tsv.gz |") or die $!;
while (my $line = <IN>) {
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

# Load external searches before.
print STDERR "Loading external searches before\n";
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
print STDERR "Loading external searches after\n";
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

# Write summary to disk.
print STDERR "Writing summary to disk\n";
open(OUT, "| gzip > $DATADIR/links_added_in_02-15_WITH-STATS.tsv.gz");
print OUT join("\t", 'src', 'tgt', 'num_source_before', 'num_source_after', 'num_paths_before',
  'num_paths_after', 'num_clicks_before', 'num_clicks_after', 'num_wiki_searches_302_before',
  'num_wiki_searches_200_before', 'num_wiki_searches_302_after', 'num_wiki_searches_200_after',
  'num_external_searches_before', 'num_external_searches_after') . "\n";
foreach my $pair (keys %new_links) {
  my ($src, $tgt) = split(/\t/, $pair);
  $num_source_before = $source_counts_before{$src} || 0;
  $num_source_after = $source_counts_after{$src} || 0;
  $num_paths_before = $path_counts_before{$pair} || 0;
  $num_paths_after = $path_counts_after{$pair} || 0;
  $num_clicks_before = $click_counts_before{$pair} || 0;
  $num_clicks_after = $click_counts_after{$pair} || 0;
  $num_wiki_searches_302_before = $wiki_search_counts_302_before{$pair} || 0;
  $num_wiki_searches_200_before = $wiki_search_counts_200_before{$pair} || 0;
  $num_wiki_searches_302_after = $wiki_search_counts_302_after{$pair} || 0;
  $num_wiki_searches_200_after = $wiki_search_counts_200_after{$pair} || 0;
  $num_external_searches_before = $external_search_counts_before{$pair} || 0;
  $num_external_searches_after = $external_search_counts_after{$pair} || 0;

  print OUT join("\t", $pair, $num_source_before, $num_source_after, $num_paths_before,
    $num_paths_after, $num_clicks_before, $num_clicks_after, $num_wiki_searches_302_before,
    $num_wiki_searches_200_before, $num_wiki_searches_302_after, $num_wiki_searches_200_after,
    $num_external_searches_before, $num_external_searches_after) . "\n";
}
close(OUT);
