#!/usr/bin/perl

$BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

my @langs = split(/\n/, `cut -f1 $BASEDIR/list_of_wikipedias.tsv`);

foreach my $lang (@langs) {
  my $url = "http://$lang.wikipedia.org/w/index.php?title=Special:ListUsers/bot&offset=&limit=500&group=bot";
  `wget -O $BASEDIR/bots/html/bots_$lang.html $url`;
}
