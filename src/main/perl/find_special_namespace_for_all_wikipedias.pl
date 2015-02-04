#!/usr/bin/perl

$BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

open(HTML, "$BASEDIR/list_of_wikipedias.tsv") or die $!;
while (my $line = <HTML>) {
  if ($line =~ /(.*)\t(.*)/) {
    my ($lang, $name) = ($1, $2);
    my $url = "http://$lang.wikipedia.org/w/api.php?action=query&meta=siteinfo&format=json&siprop=namespaces%7Cnamespacealiases";
    my $json = `wget -q -O - '$url'`;
    if ($json =~ /{"id":-1,"case":"first-letter","\*":"(.*?)","canonical":"Special"}/) {
      print "$lang\t$1\n";
    }
  }
}
close(HTML);
