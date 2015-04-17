#!/usr/bin/perl

my $BASEDIR = $ENV{'HOME'} . "/wikimedia/trunk/";

open(LANG, "$BASEDIR/data/largest_wikipedias.txt");
while (my $lang = <LANG>) {
	chomp $lang;
	`ln -s $BASEDIR/data/redirects/$lang\wiki_*_redirects.tsv.gz $BASEDIR/data/redirects/no_date/$lang\_redirects.tsv.gz`;
}
close(LANG);
