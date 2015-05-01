#DUMP_INPUT_FOLDER='/dfs/scratch1/ashwinp/revision_dump_20150403/dump'
DUMP_INPUT_FOLDER='/dfs/scratch1/ashwinp/non_en_revision_dump_20150331/dump'
#DUMP_OUTPUT_FOLDER='/dfs/scratch1/ashwinp/revision_dump_link_deltas_20150403'
DUMP_OUTPUT_FOLDER='/dfs/scratch1/ashwinp/non_en_revision_dump_link_deltas_20150331'

#Important: extract-link-deltas resolves using redirect file. The file path needs to be set in extract-link-deltas.pl
ls $DUMP_INPUT_FOLDER | parallel -j40 --delay 3 "7z e -so $DUMP_INPUT_FOLDER/"{}" | /usr/bin/perl -CS extract-link-history.pl | /usr/bin/perl -CS extract-link-deltas.pl > $DUMP_OUTPUT_FOLDER/"{.}
#| parallel -j40 --delay 3 "bzcat $DUMP_INPUT_FOLDER/"{}" | bash link_extraction_mapper.sh > $DUMP_OUTPUT_FOLDER/"{.}
