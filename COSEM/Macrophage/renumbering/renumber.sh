#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkRenumberN5
N_NODES=10

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}


ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5'  \
--inputN5DatasetName 'ecs,plasma_membrane,mito_cropped,mito_membrane,mito_skeleton_pruned_cropped,mito_skeleton_pruned_longestShortestPath_cropped,vesicle,vesicle_membrane,MVB,MVB_membrane,er,er_membrane,er_medialSurface,ERES,nucleus_cropped,nucleolus,golgi,golgi_membrane,lysosome,lysosome_membrane,LD,LD_membrane,NE,NE_membrane,nuclear_pore,chromatin,NHChrom,centrosome,distal_app,ribosomes,er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc,er_maskedWith_golgi_to_golgi_cc,er_reconstructed_to_mito_maskedWith_er_reconstructed_cc,er_to_MVB_cc,er_maskedWith_plasma_membrane_to_plasma_membrane_cc,er_to_vesicle_cc,golgi_to_vesicle_cc,golgi_to_MVB_cc,plasma_membrane_to_mito_cc' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/renumbered/${cell}.n5'  \
--inputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}' \
--renumberingCSV 'ecs,plasma_membrane,mito_cropped,mito_cropped,mito_cropped,mito_cropped,vesicle,vesicle,MVB,MVB,er,er,er,ERES,nucleus_cropped,nucleolus,golgi,golgi,lysosome,lysosome,LD,LD,NE,NE,nuclear_pore,chromatin,NHChrom,centrosome,distal_app,ribosomes,er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc,er_maskedWith_golgi_to_golgi_cc,er_reconstructed_to_mito_maskedWith_er_reconstructed_cc,er_to_MVB_cc,er_maskedWith_plasma_membrane_to_plasma_membrane_cc,er_to_vesicle_cc,golgi_to_vesicle_cc,golgi_to_MVB_cc,plasma_membrane_to_mito_cc' \
"

export N_CORES_DRIVER=1
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

sleep 2

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5'  \
--inputN5DatasetName 'er_maskedWith_microtubules_to_microtubules_cc,MVB_maskedWith_microtubules_to_microtubules_cc,golgi_maskedWith_microtubules_to_microtubules_cc,mito_maskedWith_microtubules_to_microtubules_cc_cropped,nucleus_maskedWith_microtubules_to_microtubules_cc,vesicle_maskedWith_microtubules_to_microtubules_cc,plasma_membrane_maskedWith_microtubules_to_microtubules_cc,microtubules' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/renumbered/${cell}.n5/contactDistance20.n5'  \
--inputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}/contactDistance20' \
--renumberingCSV 'er_maskedWith_microtubules_to_microtubules_cc,MVB_maskedWith_microtubules_to_microtubules_cc,golgi_maskedWith_microtubules_to_microtubules_cc,mito_maskedWith_microtubules_to_microtubules_cc_cropped,nucleus_maskedWith_microtubules_to_microtubules_cc,vesicle_maskedWith_microtubules_to_microtubules_cc,plasma_membrane_maskedWith_microtubules_to_microtubules_cc,microtubules' \
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
