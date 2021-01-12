#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCalculateSheetnessOfContactSites
N_NODES=15

ARGV="\
--inputN5ContactSiteDatasetName 'er_reconstructed_maskedWith_microtubules_to_microtubules_cc,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc' \
--inputN5SheetnessDatasetName 'er_sheetnessVolumeAveraged_maskedWith_microtubules' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Jurkat.n5/contactDistance20.n5' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Jurkat/contactDistance20' "

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
