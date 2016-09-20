#!/usr/bin/perl

# cf. http://www.mediawiki.org/wiki/Manual:Page_table

my $DATADIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

my @langs = split(/\n/, `cut -f1 $DATADIR/list_of_wikipedias.tsv`);

open(ERR, '> sqoop_page.log') or die $!;

foreach my $lang (@langs) {
  # We only do this for English, French, Spanish, Polish
  if ($lang !~ '^en|es|fr|pl$') {
    next;
  }
  print STDERR "Importing pages for $lang\n";
  print ERR "
    =========================================
    === Importing $lang
    =========================================\n";
  my $sqoop_cmd =
    "sqoop import                                                     \\
    -Dmapreduce.output.fileoutputformat.compress=false                \\
    -Dmapreduce.job.queuename=priority                                \\
    --connect jdbc:mysql://analytics-store.eqiad.wmnet/$lang\wiki     \\
    --verbose                                                         \\
    --target-dir /user/west1/pages/$lang                              \\
    --delete-target-dir                                               \\
    --as-textfile                                                     \\
    --null-string ''                                                  \\
    --null-non-string ''                                              \\
    --fields-terminated-by '\\t'                                      \\
    --escaped-by \\\\                                                 \\
    --username=research --password [[[look up wd in doc/db_pwd.txt]]]                       \\
    --split-by page_id                                                \\
    --query '
    SELECT
      page_id,
      CAST(page_title AS CHAR(255) CHARSET utf8) AS page_title,
      page_is_redirect
    FROM page
    WHERE page_namespace = 0 AND \$CONDITIONS
    '";
  print ERR `$sqoop_cmd 2>&1`;
}

close(ERR);
