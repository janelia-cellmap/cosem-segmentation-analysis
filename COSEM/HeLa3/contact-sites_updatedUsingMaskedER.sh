#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=15

#ARGV="\
#--inputPairs '\
#er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_maskedWith_ribosomes_to_ribosomes,\
#er_maskedWith_nucleus_expansion_150_maskedWith_ribosomes_to_ribosomes,\
#er_maskedWith_ribosomes_to_ribosomes,\
#er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_maskedWith_mito_to_mito,\
#er_maskedWith_nucleus_expansion_150_maskedWith_mito_to_mito,\
#er_maskedWith_mito_to_mito,\
#er_maskedWith_golgi_to_golgi,\
#er_maskedWith_plasma_membrane_to_plasma_membrane' \
#--contactDistance 10 \
#--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
#--minimumVolumeCutoff 0 \
#--skipGeneratingContactBoundaries
#"

ARGV="\
--inputPairs '\
er_reconstructed_maskedWith_ribosomes_to_ribosomes,\
er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes,\
er_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes,\
er_maskedWith_ribosomes_to_ribosomes,\
er_reconstructed_to_mito_maskedWith_er_reconstructed,\
er_reconstructed_maskedWith_nucleus_expanded_to_mito_maskedWith_er_reconstructed,\
er_maskedWith_nucleus_expanded_to_mito_maskedWith_er,\
er_to_mito_maskedWith_er' \
--contactDistance 10 \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--minimumVolumeCutoff 0 \
--skipGeneratingContactBoundaries
"


export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
