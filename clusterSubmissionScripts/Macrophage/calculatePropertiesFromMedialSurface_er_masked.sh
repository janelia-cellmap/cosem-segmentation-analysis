#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCalculatePropertiesFromMedialSurface
N_NODES=15

#for i in {'Macrophage.n5','HeLa3.n5','Jurkat.n5','Macrophage.n5'}; do cd $i; ln -s er_sheetness_maskedWith_nucleus_expansion_150 er_maskedWith_nucleus_expansion_150_sheetness; ln -s er_medialSurface_maskedWith_nucleus_expansion_150 er_maskedWith_nucleus_expansion_150_medialSurface; cd ..; done

ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5' \
--inputN5DatasetName 'er,er_maskedWith_nucleus_expansion_150' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Macrophage/' "

export RUNTIME="48:00"
export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
