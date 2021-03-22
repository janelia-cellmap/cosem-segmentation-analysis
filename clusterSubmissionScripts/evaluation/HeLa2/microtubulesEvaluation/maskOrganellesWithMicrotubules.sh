#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkApplyMaskToCleanData
N_NODES=3

for i in {validation,refinedPredictions}
do

for j in {crop1,crop2,crop3,crop4}
do

path=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2/microtubuleEvaluation/$i/${j}.n5
ARGV="--datasetToMaskN5Path '$path' \
--datasetNameToMask '\
er,\
er_maskedWith_nucleus_expanded,\
er_reconstructed,\
er_reconstructed_maskedWith_nucleus_expanded,\
nucleus,\
mito,\
MVB,\
vesicle,\
golgi,\
plasma_membrane' \
--datasetToUseAsMaskN5Path '$path' \
--datasetNameToUseAsMask 'microtubules' \
--outputN5Path '$path'"

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
sleep 2
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

done

done
