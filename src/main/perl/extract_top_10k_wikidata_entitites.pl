#!/usr/bin/perl

# The output of this file should go to data/top_wikidata_entities/top_10k_wikidata_entities.tsv.

my $DATADIR = $ENV{'HOME'} . '/wikimedia/trunk/data/top_wikidata_entities/';

my @cats = (
'People',
'History',
'Geography',
'Arts',
'Philosophy_and_religion',
'Anthropology,_psychology_and_everyday_life',
'Society_and_social_sciences',
'Biology_and_health_sciences',
'Physical_sciences',
'Technology',
'Mathematics'
);

my %top1000 = ();
my %core = ();

open(IN, "$DATADIR/wikitext/top1000.txt") or die $!;
while (my $line = <IN>) {
  if ($line =~ /#+ *(''')?\[\[d:(Q\d+)\|(.*)\]\](''')?/) {
    my ($is_core, $mid, $title) = ($1 eq '' ? 0 : 1, $2, $3);
    $top1000{$mid} = 1;
    $core{$mid} = 1 if ($is_core);
  }
}
close(IN);

# Print the header.
print join("\t", 'IMPORTANCE', 'WIKIDATA_ID', 'CATEGORY', 'DESCRIPTION') . "\n";

foreach my $cat (@cats) {
  open(IN, "$DATADIR/wikitext/$cat.txt") or die $!;
  my @cat_path = ();
  while (my $line = <IN>) {
    if ($line =~ /^==(=*) *(.*), *\d+/) {
      my ($level, $subcat) = (length($1), $2);
      $subcat =~ s/ /_/g;
      @cat_path = @cat_path[0..$level];
      $cat_path[$level] = $subcat;
    } elsif ($line =~ /#+ *(''')?\[\[d:(Q\d+)\|(.*)\]\](''')?/) {
      my ($mid, $title) = ($2, $3);
      my $score = 0;
      ++$score if ($top1000{$mid});
      ++$score if ($core{$mid});
      print join("\t", $score, $mid, join('/', @cat_path), $title) . "\n";
    }
  }
  close(IN);
}