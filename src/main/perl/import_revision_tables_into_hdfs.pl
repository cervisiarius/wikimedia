#!/usr/bin/perl

my $DATADIR = $ENV{'HOME'} . '/wikimedia/trunk/data/';

my @langs = split(/\n/, `cut -f1 $DATADIR/list_of_wikipedias.tsv`);

foreach my $lang (@langs) {
  print STDERR "Importing $lang\n";
  my $sqoop_cmd =
    "sqoop import                                                     \\
    -Dmapreduce.output.fileoutputformat.compress=false                \\
    --connect jdbc:mysql://s1-analytics-slave.eqiad.wmnet/$lang\wiki  \\
    --verbose                                                         \\
    --target-dir /user/west1/revision_history/$lang                   \\
    --delete-target-dir                                               \\
    --as-textfile                                                     \\
    --null-string ''                                                  \\
    --null-non-string ''                                              \\
    --fields-terminated-by '\\t'                                      \\
    --escaped-by \\\\                                                 \\
    --username=research --password HGY3DhGoYhxF                       \\
    --split-by a.rev_id                                               \\
    --query '
    SELECT
      a.rev_id,
      a.rev_page,
      a.rev_text_id,
      CAST(a.rev_comment AS CHAR(255) CHARSET utf8) AS rev_comment,
      a.rev_user,
      CAST(a.rev_user_text AS CHAR(255) CHARSET utf8) AS rev_user_text,
      CAST(a.rev_timestamp AS CHAR(255) CHARSET utf8) AS rev_timestamp,
      a.rev_minor_edit,
      a.rev_deleted,
      a.rev_len,
      a.rev_parent_id
    FROM revision AS a
    WHERE \$CONDITIONS
    LIMIT 1000
    '";
  `$sqoop_cmd`;
}
