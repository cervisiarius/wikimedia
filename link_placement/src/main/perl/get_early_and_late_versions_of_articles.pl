#!/usr/bin/perl

#!/usr/bin/perl -CS

# NB: Note the -CS flag above; it is such that we read and write from STDIN and STDOUT in UTF-8 format.

# Important: this assumes the input to be in exactly the same order as it was produced by
# extract_link_deltas.pl; since (I think) the link-delta extraction for enwiki20150403 did not sort
# revisions by date before processing the data (which might result in some out-of-order revisions),
# you should not sort the data by date before running this 'ere script. As long as the is in the same
# order as it went into (and thus came out of) extract_link_deltas.pl, the full list of links for a page
# will be reconstructible from the deltas.

# Input: /dfs/scratch1/ashwinp/revision_dump_link_deltas_20150403/*

use HTML::Entities;
# Important: deal with unicode properly (for uppercasing etc.).
use utf8;

# print STDERR "Loading redirects... ";
# my %redirects = ();
# open(RED, "zcat " . $ENV{'HOME'} . "/wikimedia/trunk/data/redirects/enwiki_20141008_redirects.tsv.gz |");
# while (my $line = <RED>) {
#   chomp $line;
#   my @tokens = split(/\t/, $line, 2);
#   $redirects{$tokens[0]} = $tokens[1];
# }
# close(RED);
# print STDERR "DONE\n";

sub normalize_title {
  my $title = shift;
  $title =~ s/ /_/g;
  # This would rarely do anything, since redirects were already resolved temporally when extracting
  # deltas, so skip the step.
  #my $resolved = $redirects{$title};
  #$title = $resolved if ($resolved);
  return $title;
}

my $early_date = '';
my %early_links = ();
my $late_date = '';
my %late_links = ();
my %current_links = ();

my $page_prev_title = '';
my $i = 0;

while (my $line = <STDIN>) {
  chomp $line;
  my ($page_id, $page_title, $revision_id, $revision_parentid, $revision_timestamp,
    $revision_userid, $revision_username, $revision_length, $del_link_string,
    $add_link_string) = split(/\t/, $line);
  $page_title = normalize_title($page_title);
  ++$i;
  print STDERR "$i\t$page_id\n" if ($i % 100000 == 0);
  # New page, so flush.
  if ($page_prev_title ne $page_title && $page_prev_title ne '') {
    if ($early_date ne '' && $late_date ne '') {
      print join("\t", $page_prev_title, $early_date, $late_date,
        scalar(keys %early_links), scalar(keys %late_links),
        join("|", keys %early_links), join("|", keys %late_links)) . "\n";
    }
    $early_date = '';
    %early_links = ();
    $late_date = '';
    %late_links = ();
    %current_links = ();
  }
  # Ignore redirect rows. We simply skip them, i.e., the previous link set is the one before
  # the redirect, rather than the single link contained in the redirect.
  foreach my $link (split(/\|/, $del_link_string)) {
    $link = normalize_title($link);
    delete $current_links{$link};
  }
  foreach my $link (split(/\|/, $add_link_string)) {
    $link = normalize_title($link);
    $current_links{$link} = 1;
  }
  # Store latest revision before January.
  if (($revision_timestamp cmp '2014-12-31') <= 0 && ($early_date eq '' || ($revision_timestamp cmp $early_date) >= 0)) {
    $early_date = $revision_timestamp;
    %early_links = %current_links;
  }
  # Store latest revision before March.
  elsif (($revision_timestamp cmp '2015-02-29') <= 0 && ($late_date eq '' || ($revision_timestamp cmp $late_date) >= 0)) {
    $late_date = $revision_timestamp;
    %late_links = %current_links;
  }
  # Remember the currect values as previous values for the next step.
  $page_prev_title = $page_title;
}

# Process last.
if ($early_date ne '' && $late_date ne '') {
  print join("\t", $page_prev_title, $early_date, $late_date,
    join("|", keys %early_links), join("|", keys %late_links)) . "\n";
}
