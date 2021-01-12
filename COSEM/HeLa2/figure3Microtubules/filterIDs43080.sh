#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=3

for datasetName in {microtubules,er,er_maskedWith_nucleus_expanded,nucleus,MVB,mito,vesicle,er_maskedWith_microtubules_to_microtubules_cc,nucleus_maskedWith_microtubules_to_microtubules_cc,er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc,MVB_maskedWith_microtubules_to_microtubules_cc,mito_maskedWith_microtubules_to_microtubules_cc,vesicle_maskedWith_microtubules_to_microtubules_cc}
do

idsToKeep=''
case $datasetName in

	microtubules) idsToKeep=43080 ;;
	er) idsToKeep= 58756962048 ;;
	er_maskedWith_nucleus_expanded) idsToKeep=58756962048;;
	nucleus) idsToKeep=76655033983 ;;
	MVB) idsToKeep=33180859540,31299247694 ;;
	mito) idsToKeep=78566208001,106884288001 ;;
	vesicle) idsToKeep=11141372188,11544692144 ;;
	er_maskedWith_microtubules_to_microtubules_cc) idsToKeep=26115559799,35215663618,11391104146,35772319581,35254051620,30780895732,35196463622,15537979949,35023747636,35714731583,28035343776,33180883703,35350027604,25482019806,27785839776,32892835705 ;;
	er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc) idsToKeep=26115559799,35215663618,11391104146,35254051620,30780895732,35196463622,15537979949,35023747636,28035343776,33180883703,25482019806,27785839776,32892835705 ;;
	nucleus_maskedWith_microtubules_to_microtubules_cc) idsToKeep=35503735596 ;;
	MVB_maskedWith_microtubules_to_microtubules_cc) idsToKeep=34006315684,31913719708 ;;
	mito_maskedWith_microtubules_to_microtubules_cc) idsToKeep=16592252400,26437421219 ;;
	vesicle_maskedWith_microtubules_to_microtubules_cc) idsToKeep=11045372184,11448680145 ;;

esac

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/43080.n5'  \
--outputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/43080.n5/idFiltered.n5'
--inputN5DatasetName '$datasetName' \
--idsToKeep '$idsToKeep' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2

done
