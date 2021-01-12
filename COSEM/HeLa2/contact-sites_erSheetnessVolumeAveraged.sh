#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=15

ARGV="\
--inputN5DatasetName 'er_sheetnessVolumeAveraged_ccAt1SkipSmoothing,mito' \
--contactDistance 10 \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5' \
--minimumVolumeCutoff 0
"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
