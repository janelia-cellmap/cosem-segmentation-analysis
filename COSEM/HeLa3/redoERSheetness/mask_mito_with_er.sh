#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkApplyMaskToCleanData
N_NODES=15

#ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
#--datasetNameToMask 'er,er_maskedWith_nucleus_expansion_150,er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150,er_sheetness_maskedWith_nucleus_expansion_150,er_medialSurface_maskedWith_nucleus_expansion_150' \
#--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
#--datasetNameToUseAsMask 'ribosomes' \
#--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5'"

ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--datasetNameToMask 'mito' \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--datasetNameToUseAsMask 'er_reconstructed' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5'"

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
