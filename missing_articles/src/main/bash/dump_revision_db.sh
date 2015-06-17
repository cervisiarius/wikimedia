#!/bin/sh

mysqldump -u research -p -h analytics-store.eqiad.wmnet \
--single-transaction                                    \
--no-create-db --no-create-info                         \
--max_allowed_packet=536870912                          \
--net_write_timeout=600                                 \
--net_read_timeout=600                                  \
enwiki revision                                         \
| sed 's/),(/\n/g'                                      \
| bzip2 > enwiki_revison.sql.bz2
