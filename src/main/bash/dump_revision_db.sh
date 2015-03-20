#!/bin/sh

mysqldump -u research -p -h analytics-store.eqiad.wmnet \
--single-transaction                                    \
--no-create-db --no-create-info                         \
enwiki revision                                         \
| sed 's/),(/\n/g'                                      \
| bzip2 > enwiki_revison.sql.bz2

HGY3DhGoYhxF
