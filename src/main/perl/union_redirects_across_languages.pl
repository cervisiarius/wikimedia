#!/usr/bin/perl

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
