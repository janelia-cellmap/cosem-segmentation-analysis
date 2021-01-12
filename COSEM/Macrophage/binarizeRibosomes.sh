#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkBinarize
N_NODES=15

cell=${PWD##*/} 
for dataset in {ribosomes_nuclear,ribosomes_cytosolic,ribosomes_sheet,ribosomes_tube}
do


INPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5"

ARGV="\
--inputN5Path '$INPUTN5PATH' \
--inputN5DatasetName '${dataset}' \
--keepAsLong \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2


done

