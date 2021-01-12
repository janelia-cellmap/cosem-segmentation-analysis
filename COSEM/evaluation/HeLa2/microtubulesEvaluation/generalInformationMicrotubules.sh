#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2/microtubuleEvaluation
for i in {validation,refinedPredictions}
do

for j in {1,2,3,4}
do

INPUTDATASETS=microtubules
#if [[ "$i" == "refinedPredictions" ]];
#        then 
INPUTPAIRS=er_maskedWith_microtubules_to_microtubules,er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules,er_reconstructed_maskedWith_microtubules_to_microtubules,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules,golgi_maskedWith_microtubules_to_microtubules,mito_maskedWith_microtubules_to_microtubules,MVB_maskedWith_microtubules_to_microtubules,nucleus_maskedWith_microtubules_to_microtubules,plasma_membrane_maskedWith_microtubules_to_microtubules,vesicle_maskedWith_microtubules_to_microtubules
INPUTDATASETS=$INPUTDATASETS,er_maskedWith_microtubules,er_maskedWith_nucleus_expanded_maskedWith_microtubules,er_reconstructed_maskedWith_microtubules,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules,golgi_maskedWith_microtubules,mito_maskedWith_microtubules,MVB_maskedWith_microtubules,nucleus_maskedWith_microtubules,plasma_membrane_maskedWith_microtubules,vesicle_maskedWith_microtubules
#	else INPUTPAIRS=er_to_microtubules,golgi_to_microtubules,mito_to_microtubules,MVB_to_microtubules,nucleus_to_microtubules,plasma_membrane_to_microtubules,vesicle_to_microtubules
#fi
	ARGV="\
	--inputPairs '$INPUTPAIRS' \
	--inputN5DatasetName '$INPUTDATASETS' \
	--skipContactSites
	--inputN5Path '$BASENAME/${i}/crop${j}.n5' \
	--outputDirectory '$BASENAME/analysisResults/$i/crop$j'
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
	sleep 2 
done

done
