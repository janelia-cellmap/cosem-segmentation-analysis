#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
export N_CORES_DRIVER=1
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=15

#datasetnames=mito
cell=${PWD##*/}

ARGV="--n5PathToCropTo '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
--datasetNameToCropTo 'er' \
--inputN5Path  '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5' \
--inputN5DatasetName 'mito_maskedWith_microtubules_to_microtubules_cc' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5' \
--outputN5DatasetSuffix '_cropped'
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV 
sleep 2

