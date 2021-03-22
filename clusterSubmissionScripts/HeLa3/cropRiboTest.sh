#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=3

path=/nrs/cosem/cosem/training/v0003.2/setup01/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm_it1200000.n5

ARGV="--dimensions '540,540,540' \
--offsetsToCropTo '22320,1440,7200' \
--blockSize '180,180,180' \
--inputN5Path  '$path' \
--inputN5DatasetName 'ribosomes' \
--outputN5Path '/groups/cosem/cosem/ackermand/testRibo.n5' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2


