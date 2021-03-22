#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkGetRenumbering
N_NODES=10

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}

#first do non microtubules (those that are not in contactDistance20.n5 directory)

#ARGV="\
#--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5'  \
#--inputN5DatasetName 'ecs,plasma_membrane,mito_cropped,vesicle,MVB,er,ERES,nucleus_cropped,nucleolus,golgi,lysosome,LD,NE,nuclear_pore,chromatin,NHChrom,centrosome,distal_app,ribosomes,er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc,er_maskedWith_golgi_to_golgi_cc,er_reconstructed_to_mito_maskedWith_er_reconstructed_cc,er_to_MVB_cc,er_maskedWith_plasma_membrane_to_plasma_membrane_cc,er_to_vesicle_cc,golgi_to_vesicle_cc,golgi_to_MVB_cc,plasma_membrane_to_mito_cc' \
#--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}'
#"

#export N_CORES_DRIVER=1
#export RUNTIME="48:00"
#TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

sleep 2

#then do microtubules
ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5'  \
--inputN5DatasetName 'microtubules,er_maskedWith_microtubules_to_microtubules_cc,MVB_maskedWith_microtubules_to_microtubules_cc,golgi_maskedWith_microtubules_to_microtubules_cc,mito_maskedWith_microtubules_to_microtubules_cc_cropped,nucleus_maskedWith_microtubules_to_microtubules_cc,vesicle_maskedWith_microtubules_to_microtubules_cc,plasma_membrane_maskedWith_microtubules_to_microtubules_cc' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}/contactDistance20'
"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
