#!/bin/sh

mysqldump -u research -p [[[look up wd in doc/db_pwd.txt]]] -h analytics-store.eqiad.wmnet \
--no-create-db --no-create-info                         \
--single-transaction --quick                            \
--max_allowed_packet=512000000                          \
enwiki revision                                         \
| sed 's/),(/\n/g'                                      \
| bzip2 > enwiki_revison.sql.bz2

