#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandDataset
N_NODES=2

BASEPATH=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Jurkat/

for i in {validation,refinedPredictions}
do

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

ARGV="--inputN5DatasetName 'ribosomes_centers' \
--inputN5Path '$BASEPATH/${i}/${j}CC.n5' \
--expansionInNm 10 \
--outputN5DatasetSuffix '_expansion_10'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2 

if [ ! -d $BASEPATH/${i}/${j}CC.n5/ribosomes ]; then
ln -s $BASEPATH/${i}/${j}CC.n5/ribosomes_centers_expansion_10 $BASEPATH/${i}/${j}CC.n5/ribosomes
fi

done

done

