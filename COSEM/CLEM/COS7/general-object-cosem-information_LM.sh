#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=10

ARGV="--inputN5DatasetName 'er_cc,mito_membrane_cc' \
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/imageData/LoadID277_Cell11_PALM.n5' \
--outputDirectory '/groups/cosem/cosem/ackermand/CLEM/analysisResults/LoadID277_Cell11_PALM'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
