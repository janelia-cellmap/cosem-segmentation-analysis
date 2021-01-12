#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=3

validationPath=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2/microtubuleEvaluation/validation
if [ ! -d $validationPath ]; then
	mkdir -p $validationPath
fi

for j in {refinedPredictions,validation}
do

for i in {1,2,3,4}
do

datasetnames=vesicle

path=$validationPath/crop${i}.n5

#if [ ! -d $path ]; then
#        mkdir $path
#fi

#if [ ! -d $path/microtubules_centerAxis ]; then
#	cp -r /nrs/funke/ecksteinn/micron_experiments/cosem_hela_2_full/00_data/exports/cosem_hela_2_gt_blocks.n5/block_$i $path/microtubules_centerAxis
 #manually copying and switching z and x       
#fi

ARGV="--n5PathToCropTo '$path' \
--datasetNameToCropTo 'microtubules_centerAxis' \
--inputN5Path  '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5' \
--inputN5DatasetName '$datasetnames' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2/microtubuleEvaluation/${j}/crop${i}.n5' \
--outputN5DatasetSuffix '' \
"

export RUNTIME="48:00"
sleep 2
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

done

done
