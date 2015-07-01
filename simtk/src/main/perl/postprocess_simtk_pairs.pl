#!/usr/bin/perl

use Digest::MD5 qw(md5 md5_hex md5_base64);

my $DATADIR = $ENV{'HOME'} . "/wikimedia/trunk/data/simtk/pair_counts/";

my %click_counts = ();
my %pair_counts = ();
my %session_memberships = ();

# Load direct clicks.
open(CLICKS, "gunzip -c $DATADIR/clicks.tsv.gz |") or die $!;
while (my $pair = <CLICKS>) {
  chomp $pair;
  my ($src, $tgt) = split(/\t/, $pair);
  if ($src =~ m{^/home/[^/]+/$} && $tgt =~ m{^/home/[^/]+/$}) {
    ++$click_counts{$pair};
  }
}
close(CLICKS);

# Load pairs appearing in same session.
open(PAIRS, "gunzip -c $DATADIR/pairs.tsv.gz |") or die $!;
while (my $pair = <PAIRS>) {
  chomp $pair;
  my ($src, $tgt) = split(/\t/, $pair);
  if ($src =~ m{^/home/[^/]+/$} && $tgt =~ m{^/home/[^/]+/$}) {
    ++$pair_counts{$pair};
  }
}
close(PAIRS);

# Print the candidate scores.
open(OUT, "| gzip > $DATADIR/link_candidates_scores_GROUND-TRUTH.tsv.gz");
foreach my $pair (keys %click_counts) {
  print OUT join("\t", $pair, $click_counts{$pair}/$pair_counts{$pair}, $pair_counts{$pair}) . "\n";
}
close(OUT);

# Load session memberships.
open(SESS, "gunzip -c $DATADIR/sessions.tsv.gz |") or die $!;
while (my $line = <SESS>) {
  chomp $line;
  my ($src, $tgt, $session_id) = split(/\t/, $line);
  my $pair = "$src\t$tgt";
  if ($src =~ m{^/home/[^/]+/$} && $tgt =~ m{^/home/[^/]+/$} && $session_id =~ /(.*)(_\d+)/) {
    my $session_id = md5_hex($1) . $2;
#    my $session_id = $1 . $2;
    if (defined $session_memberships{$pair}) {
      $session_memberships{$pair}->{$session_id} = 1;
    } else {
      my $hash_ref;
      $hash_ref->{$session_id} = 1;
      $session_memberships{$pair} = $hash_ref;
    }
  }
}
close(SESS);

# Print session memberships in the format:
# src, tgt, comma-separated list of sessions in which this candidate appears.
open(OUT, "| gzip > $DATADIR/link_candidates_tree_ids.tsv.gz");
foreach my $pair (keys %session_memberships) {
  print OUT "$pair\t";
  my $sep = '';
  my $hash_ref = $session_memberships{$pair};
  foreach my $session_id (keys %$hash_ref) {
    print OUT "$sep$session_id";
    $sep = ',';
  }
  print OUT "\n";
}
close(OUT);
