#!/usr/bin/perl

$BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

open(HTML, "$BASEDIR/list_of_wikipedias.html") or die $!;
while (my $line = <HTML>) {
  # <tr><td><a id="aa" name="aa"></a><strong title="Afar"><span lang="aa">Qafár af</span> (Afar)</strong></td><td><del><a href="//aa.wikipedia.org">aa</a></del></td><td><del><a href="//aa.wiktionary.org">aa</a></del></td><td><del><a href="//aa.wikibooks.org">aa</a></del></td><td><a href="//aa.wikinews.org" class="new">aa</a></td><td><a href="//aa.wikiquote.org" class="new">aa</a></td><td><a href="//aa.wikisource.org" class="new">aa</a></td><td><a href="//aa.wikiversity.org" class="new">aa</a></td><td><a href="//aa.wikivoyage.org" class="new">aa</a></td></tr>
  if ($line =~ m{<strong title="(.*?)"><span lang="(.*?)">.*?</span> \(.*?\)</strong></td><td><a href="//[^.]*\.wikipedia\.org">.*?</a></td>}) {
    my ($lang, $name) = ($2, $1);
    print "$lang\t$name\n";
  }
}
close(HTML);
