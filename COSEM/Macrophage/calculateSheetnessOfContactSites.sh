#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCalculateSheetnessOfContactSites
N_NODES=10

ARGV="\
--inputN5ContactSiteDatasetName 'er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_to_mito_cc,er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_to_ribosomes_cc' \
--inputN5SheetnessDatasetName 'er_maskedWith_nucleus_expansion_150_sheetnessVolumeAveraged' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Macrophage/' "

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
