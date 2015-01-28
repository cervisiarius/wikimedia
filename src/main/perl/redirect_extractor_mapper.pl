#!/usr/bin/perl

my $title = '';
while (my $line = <STDIN>) {
  if ($line =~ m{^\s*<title>(.*)</title>\s*$}) {
    $title = $1;
  }
  if ($line =~ m{^\s*<redirect title="(.*)"\s*/>}) {
    my $target = $1;
    if ($title ne '' && $target ne '') {
      $title =~ s/\s/_/g;
      $target =~ s/\s/_/g;
      $target =~ s/\\"/"/g;
      print "$title\t$target\n";
    }
  }
  if ($line =~ m{^\s*</page>\s*$}) {
    $title = '';
  }
}
