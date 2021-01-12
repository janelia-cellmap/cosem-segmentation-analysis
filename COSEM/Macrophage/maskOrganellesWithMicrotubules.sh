#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkApplyMaskToCleanData
N_NODES=15

cell=${PWD##*/}

ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
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
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
--datasetNameToUseAsMask 'microtubules' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5'"

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 2
mkdir /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5
for i in {er,er_maskedWith_nucleus_expanded,er_reconstructed,er_reconstructed_maskedWith_nucleus_expanded,nucleus,mito,MVB,vesicle,golgi,plasma_membrane}; do
	ln -s /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/${i}_maskedWith_microtubules /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5/
done

ln -s /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/microtubules /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5/
