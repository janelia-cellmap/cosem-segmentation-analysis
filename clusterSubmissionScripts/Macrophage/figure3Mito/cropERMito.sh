#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=3

path=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5
datasetnames=raw,er,er_medialSurface,er_sheetness,er_sheetnessVolumeAveraged,er_reconstructed,mito

ARGV="
--dimensions '501,501,501' \
--offsetsToCropTo '24040,1320,8800' \
--blockSize '128,128,128' \
--inputN5Path  '$path' \
--inputN5DatasetName '$datasetnames' \
--outputN5Path '/groups/cosem/cosem/ackermand/figure3Mito.n5/Macrophage.n5' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2


