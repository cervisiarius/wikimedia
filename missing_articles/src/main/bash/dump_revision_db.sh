#!/bin/sh

mysqldump -u research -p -h analytics-store.eqiad.wmnet \
--single-transaction                                    \
--no-create-db --no-create-info                         \
--max_allowed_packet=8000000                            \
enwiki revision                                         \
| sed 's/),(/\n/g'                                      \
| bzip2 > enwiki_revison.sql.bz2
