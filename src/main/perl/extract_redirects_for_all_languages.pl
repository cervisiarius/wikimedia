#!/usr/bin/perl

$BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

open(HTML, "$BASEDIR/list_of_wikipedias.tsv") or die $!;
while (my $line = <HTML>) {
  if ($line =~ /(.*)\t(.*)/) {
    my ($lang, $name) = ($1, $2);
    my $ls = `hadoop fs -ls /dataset/wikipedia_dumps | grep '/dataset/wikipedia_dumps/$lang\wiki-.*-pages-articles-multistream\.xml\.bz2'`;
    if ($ls =~ m{.*(/dataset/wikipedia_dumps/$lang\wiki-(.*)-pages-articles-multistream\.xml\.bz2)}) {
      my ($dump_file, $date) = ($1, $2);
      print STDERR "Processing $lang ... ";
      `hadoop fs -cat $dump_file | bunzip2 | perl ./redirect_extractor_mapper.pl | gzip > $BASEDIR/redirects/$lang\wiki_$date\_redirects.tsv.gz`;
      print STDERR "DONE\n";
    }
  }
}
close(HTML);
