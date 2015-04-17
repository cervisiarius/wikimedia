#!/bin/sh

export BASEDIR=$HOME/wikimedia/trunk
export LANGUAGE=$1

gunzip -c $BASEDIR/data/revision_history_aggregated/revision_history_$LANGUAGE.tsv.gz \
| python $BASEDIR/src/main/python/vw_revision_history_formatter.py $LANGUAGE \
| vw /dev/stdin -b 18 -q ui --rank 1000 --l2 0.001 \
  --learning_rate 0.015 --passes 2 --decay_learning_rate 0.97 --power_t 0 \
  -f vw_$LANGUAGE.reg --cache_file vw_$LANGUAGE.cache
