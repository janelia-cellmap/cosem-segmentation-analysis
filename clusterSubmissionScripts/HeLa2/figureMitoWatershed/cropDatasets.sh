#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=5


ARGV="--dimensions '1000,500,1000' \
--offsetsToCropTo '40000,1000,20000' \
--blockSize '128,128,128' \
--inputN5Path  '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5' \
--inputN5DatasetName 'raw,mito_renumbered,mito_smoothed_fragments_renumbered,mito_naiveCC_filled_renumbered,mito_prediction' \
--outputN5Path '/groups/cosem/cosem/ackermand/figureMitoWatershed16Bit.n5' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2


