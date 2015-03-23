#!/usr/bin/perl

$BASEDIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

my @files = split(/\n/, `ls $BASEDIR/bots/html`);

my %all_bots = ();

foreach my $file (@files) {
  my $html = `cat $BASEDIR/bots/html/$file`;
  $html =~ s/<\/li>/<\/li>\n/g;
  for my $line (split /\n/, $html) {
    if ($line =~ m{<li><a href="/wiki/.+?" title=".+?" class="mw-userlink">(.+?)</a> <span class="mw-usertoollinks">(.*?)</span>‏‎.*?</li>}) {
      $all_bots{$1} = 1;
    }
  }
}

foreach my $bot (sort {$a cmp $b} keys(%all_bots)) {
  print "$bot\n";
}
