#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=3

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2
for i in {validation,rawPredictions,refinedPredictions}
do

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

if [[ "$i" == "refinedPredictions" ]];
        then INPUTPAIRS=er_maskedWith_microtubules_to_microtubules,er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules,er_reconstructed_maskedWith_microtubules_to_microtubules,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules,golgi_maskedWith_microtubules_to_microtubules,mito_maskedWith_microtubules_to_microtubules,MVB_maskedWith_microtubules_to_microtubules,nucleus_maskedWith_microtubules_to_microtubules,plasma_membrane_maskedWith_microtubules_to_microtubules,vesicle_maskedWith_microtubules_to_microtubules
	else INPUTPAIRS=er_to_microtubules,golgi_to_microtubules,mito_to_microtubules,MVB_to_microtubules,nucleus_to_microtubules,plasma_membrane_to_microtubules,vesicle_to_microtubules
fi

	ARGV="\
	--inputPairs '$INPUTPAIRS' \
	--contactDistance 20 \
	--inputN5Path '$BASENAME/$i/${j}CC.n5' \
	--outputN5Path '$BASENAME/$i/${j}CC.n5/contactDistance20.n5'	
	--minimumVolumeCutoff 0 \
	--skipGeneratingContactBoundaries
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
	sleep 2
done

done
