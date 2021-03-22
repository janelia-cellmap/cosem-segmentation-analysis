#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=3

BASEPATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Macrophage/'

export RUNTIME="48:00"

for i in {validation,rawPredictions,refinedPredictions}
do

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

if [[ "$i" == "rawPredictions" ]]; then RIBOSOMES=ribosomes; else RIBOSOMES=ribosomes_centers; fi
if [[ "$i" == "refinedPredictions" ]]; 
	then MITOANDER=mito,er,er_reconstructed,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes,er_maskedWith_nucleus_expanded,er_maskedWith_nucleus_expanded_maskedWith_ribosomes,er_reconstructed_maskedWith_nucleus_expanded,mito_maskedWith_er_reconstructed,mito_maskedWith_er; 
	else MITOANDER=mito,er; 
fi


ARGV="\
--inputN5DatasetName 'plasma_membrane,mito_membrane,golgi,vesicle,MVB,nucleus,$MITOANDER,$RIBOSOMES' \
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
