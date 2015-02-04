#!/usr/bin/perl

$BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

open(HTML, "$BASEDIR/list_of_wikipedias.tsv") or die $!;
while (my $line = <HTML>) {
  if ($line =~ /(.*)\t(.*)/) {
    my ($lang, $name) = ($1, $2);
    my $ls = `hadoop fs -ls /dataset/wikipedia_dumps | grep '/dataset/wikipedia_dumps/$lang\wiki-.*-pages-articles-multistream\.xml\.bz2'`;
    if ($ls =~ m{.*(/dataset/wikipedia_dumps/$lang\wiki-.*-pages-articles-multistream\.xml\.bz2)}) {
      my $dump_file = $1;
      print `hadoop fs -cat $dump_file | bunzip2 | head`;
    }
  }
}
close(HTML);
