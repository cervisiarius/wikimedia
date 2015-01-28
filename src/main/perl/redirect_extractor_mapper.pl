#!/usr/bin/perl

my $title = '';
while (my $line = <STDIN>) {
  if ($line =~ m{^\s*<title>(.*)</title>\s*$}) {
    $title = $1;
  }
  if ($line =~ m{^\s*<text xml:space="preserve">\s*#REDIRECT\s*\[\[(.*?)(\||\]\])}) {
    my $target = $1;
    if ($title ne '' && $target ne '') {
      print "$title\n$target\n";
    }
  }
  if ($line =~ m{^\s*</page>\s*$}) {
    $title = '';
  }
}