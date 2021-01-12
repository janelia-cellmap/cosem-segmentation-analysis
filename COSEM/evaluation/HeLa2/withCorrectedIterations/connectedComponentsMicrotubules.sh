#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=3

BASEPATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2/'

export RUNTIME="48:00"

i=refinedPredictions #{rawPredictions,refinedPredictions} #{validation,rawPredictions,refinedPredictions}

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

updatedData=vesicle_maskedWith_microtubules

ARGV="\
--inputN5DatasetName '$updatedData' \
--skipSmoothing \
--minimumVolumeCutoff 0 \
--thresholdIntensityCutoff 1 \
--outputN5DatasetSuffix '' \
--inputN5Path '$BASEPATH/${i}/${j}.n5' \
--outputN5Path '$BASEPATH/${i}/${j}CC.n5' \
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

sleep 2

done

done
