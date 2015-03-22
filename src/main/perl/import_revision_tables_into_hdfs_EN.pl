#!/usr/bin/perl

# The enwiki database is much larger and much more used than the others, so importing it via Sqoop
# is infeasibly. Instead, we dump it to a text file (cf. bash/dump_revision_db.sh) and copy the
# result to HDFS in this present script.

while(my $line = <STDIN>) {
  # 519207840,0,522621941,'',17565405,'Morty','20121004205613',0,0,2814,0,'p4zyhqcbdim4yx5hpf7usvrf5zdzh6h',NULL,NULL
  if ($line =~ /(\d+),(\d+),(\d+),'(.*)',(\d+),'(.*)','(\d+)',(\d+),(\d+),(\d+),(\d+),'.*',.*,.*$/) {
    my ($id, $page, $text_id, $comment, $user, $user_text, $timestamp, $minor_edit, $deleted, $len,
      $parent_id) = ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
    $user_text =~ s/\t/\\t/g;
    $comment =~ s/\t/\\t/g;
    print join("\t", $id, $page, $text_id, $user, $user_text, $timestamp, $minor_edit, $deleted,
      $len, $parent_id, $comment) . "\n";
  }
}
