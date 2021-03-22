#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkProcessLightData
N_NODES=10

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/imageData/LoadID277_Cell11_PALM.n5' \
--minimumVolumeCutoff 0 \
--minimumContactSiteVolumeCutoff 0 \
--inputN5DatasetName 'er,mito_membrane' \
--thresholdIntensity '0.08,0.31' "

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
