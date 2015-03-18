#!/usr/bin/perl

# Takes the redirect tables for the separate languages and unions them.
# Each entry also gets the language code it belongs to.

$DATADIR = $ENV{'HOME'} . '/wikimedia/trunk/data/redirects/';

my @files = split(/\s/, `ls $DATADIR`);

foreach my $file (@files) {
  if ($file =~ /([a-z]+)wiki_\d+_redirects\.tsv\.gz/) {
    my $lang = $1;
    open(IN, "zcat $DATADIR/$file |");
    while (my $line = <IN>) {
      print "$lang\t$line";
    }
    close(IN);
  }
}
