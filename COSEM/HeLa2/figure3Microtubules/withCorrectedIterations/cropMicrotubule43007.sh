#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=3

for i in {1,2}
do
path=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5
datasetnames=vesicle

if [ "$i" -eq "2" ]
then
path=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5/contactDistance20.n5
datasetnames=vesicle_maskedWith_microtubules_to_microtubules_cc
fi

ARGV="--dimensions '281,219,490' \
--offsetsToCropTo '32922,1128,1324' \
--blockSize '128,128,128' \
--inputN5Path  '$path' \
--inputN5DatasetName '$datasetnames' \
--outputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/43007.n5' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2

done

