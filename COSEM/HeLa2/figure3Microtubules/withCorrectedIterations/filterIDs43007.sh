#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=3


for datasetName in {vesicle,vesicle_maskedWith_microtubules_to_microtubules_cc}
do


idsToKeep=''
case $datasetName in

	microtubules) idsToKeep=43007 ;;
	er) idsToKeep=58756962048 ;;
	er_maskedWith_nucleus_expanded) idsToKeep=58756962048;;
	nucleus) idsToKeep=0 ;;
	MVB) idsToKeep=8567504421,12446324308 ;;
	mito) idsToKeep=48755392001 ;;
	vesicle) idsToKeep=8202548409,9047288399,12811064327,9277808365 ;;
	er_maskedWith_microtubules_to_microtubules_cc) idsToKeep=9834680369,13137668316 ;;
	er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc) idsToKeep=9834680369,13137668316 ;;
	nucleus_maskedWith_microtubules_to_microtubules_cc) idsToKeep=0 ;;
	MVB_maskedWith_microtubules_to_microtubules_cc) idsToKeep=8529008417,12369524314 ;;
	mito_maskedWith_microtubules_to_microtubules_cc) idsToKeep=11081906157 ;;
	vesicle_maskedWith_microtubules_to_microtubules_cc) idsToKeep=8260184413,9143420387,12753548319,9335480372 ;;

esac

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/43007.n5'  \
--outputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/43007.n5/idFiltered.n5'
--inputN5DatasetName '$datasetName' \
--idsToKeep '$idsToKeep' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2

done
