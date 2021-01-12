#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandDataset
N_NODES=30


ARGV="--inputN5DatasetName 'nucleus' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5' \
--expansionInNm 200 \
--outputN5DatasetSuffix '_expansion_200'"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
