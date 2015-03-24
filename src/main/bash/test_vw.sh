#!/bin/sh

export BASEDIR=$HOME/wikimedia/trunk
export LANGUAGE=$1

gunzip -c $BASEDIR/data/revision_history_aggregated/revision_history_$LANGUAGE.tsv.gz \
| python $BASEDIR/src/main/python/vw_revision_history_formatter.py $LANGUAGE \
| grep '^0' \
| vw /dev/stdin -i vw_$LANGUAGE.reg -t -p 'test.out'
