#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCalculatePropertiesFromMedialSurface
N_NODES=15

ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--inputN5DatasetName '\
er_sheetnessVolumeAveraged,\
er_sheetnessVolumeAveraged_maskedWith_nucleus_expanded,\
er_sheetnessVolumeAveraged_maskedWith_ribosomes,\
er_sheetnessVolumeAveraged_maskedWith_nucleus_expanded_maskedWith_ribosomes' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/HeLa3/' \
--calculateAreaAndVolumeFromExistingDataset "

export RUNTIME="48:00"
#export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
