#!/usr/bin/perl -w
use strict;

# usage: hash_join.pl fileWithKeysToKeep colIndexInStdinToJoinOn colIndexInFileToJoinOn
# NB: indices are 1-based

my $file_name = shift or die;
die if (not -e $file_name);
my $field_index = shift or die;
my $hash_index = shift or die;

my %hash = ();
open(HASH,$file_name) or die $!;
while(<HASH>)
{
  my $line = $_;
  chomp $line;
  my @parts = split(/\t/,$line);
  # remove whitespaces in key:
  # $_ =~ s/\s+//g;
  $hash{$parts[$hash_index - 1]} = 1;
}
close HASH;

while(<STDIN>)
{
  my $line = $_;
  chomp $line;
  my @parts = split(/\t/,$line);
  print "$line\n" if (exists $hash{$parts[$field_index - 1]});
}# end: loop over STDIN

