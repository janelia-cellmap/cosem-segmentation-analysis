#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=15

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5'

ARGV="--inputN5DatasetName 'ribosomes,ribosomes_cytosolic,ribosomes_nuclear,ribosomes_sheet,ribosomes_tube' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Macrophage/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
