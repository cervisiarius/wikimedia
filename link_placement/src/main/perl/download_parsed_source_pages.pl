#!/usr/bin/perl

use URI::Escape;
use HTML::Entities;
use Digest::MD5 qw(md5 md5_hex md5_base64);
use Time::HiRes qw(usleep);

if ($#ARGV < 1) {
  die "Specify a bucket number and a number of buckets!\n";
}

my $BUCKET = $ARGV[0];
my $NUM_BUCKETS = $ARGV[1];

my $INDIR = $ENV{'HOME'} . "/wikimedia/trunk/data/link_placement/";
my $OUTDIR = "/lfs/local/0/west1/wiki_parsed/";

my %sources = ();

open(IN, "gunzip -c $INDIR/links_added_in_02-15.tsv.gz | cut -f1 | uniq |") or die $!;
while (my $art = <IN>) {
  chomp $art;
  $sources{$art} = 1;
}
close(IN);

sub get_bucket {
  my $s = shift;
  $s = md5_hex($s);
  my $sum = 0;
  for (my $k = 0; $k < length($s); ++$k) {
    $sum += ord(substr($s, $k, 1));
  }
  return $sum % $NUM_BUCKETS;
}

# Load added links.
my $i = 1;
open(OUT, "> $OUTDIR/wiki_html_$BUCKET-of-$NUM_BUCKETS.tsv") or die $!;
foreach my $art (sort (keys %sources)) {
  if (get_bucket($art) == $BUCKET) {
    print STDERR "($i) ". decode_entities($art) ."\n";
    my $encoded_art = uri_escape($art);
    my $url = "https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvstart=20150131235959&titles=".$encoded_art."&rvprop=ids|timestamp|content&rvparse&format=json&rawcontinue";
    my $html = `curl -s "$url"`;
    print OUT "$art\t$html\n";
    ++$i;
    usleep(500*1000);
  }
}
close(OUT);
