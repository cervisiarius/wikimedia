#!/usr/bin/perl

# cf. http://www.mediawiki.org/wiki/Manual:Page_table

my $DATADIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

my @langs = split(/\n/, `cut -f1 $DATADIR/list_of_wikipedias.tsv`);

open(ERR, '> sqoop_page.log') or die $!;

foreach my $lang (@langs) {
  print STDERR "Importing $lang\n";
  print ERR "
    =========================================
    === Importing $lang
    =========================================\n";
  my $sqoop_cmd =
    "sqoop import                                                     \\
    -Dmapreduce.output.fileoutputformat.compress=false                \\
    --connect jdbc:mysql://analytics-store.eqiad.wmnet/$lang\wiki     \\
    --verbose                                                         \\
    --target-dir /user/west1/pages/$lang                              \\
    --delete-target-dir                                               \\
    --as-textfile                                                     \\
    --null-string ''                                                  \\
    --null-non-string ''                                              \\
    --fields-terminated-by '\\t'                                      \\
    --escaped-by \\\\                                                 \\
    --username=research --password HGY3DhGoYhxF                       \\
    --split-by page_id                                                \\
    --where 'page_namespace = 0'                                      \\
    --query '
    SELECT
      page_id,
      CAST(page_title AS CHAR(255) CHARSET utf8) AS page_title,
      page_is_redirect
    FROM page
    WHERE \$CONDITIONS
    '";
  print ERR `$sqoop_cmd 2>&1`;
}

close(ERR);
