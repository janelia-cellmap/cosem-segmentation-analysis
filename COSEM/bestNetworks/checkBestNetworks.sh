#!/bin/bash

for cell in {HeLa2,HeLa3,Jurkat,Macrophage}; do
	for dataset in {nucleolus,ecs,plasma_membrane,mito,mito_membrane,vesicle,vesicle_membrane,MVB,MVB_membrane,er,er_membrane,ERES,microtubules,nucleus,golgi,golgi_membrane,lysosome,lysosome_membrane,LD,LD_membrane,NE,NE_membrane,nuclear_pore,nuclear_pore_out,chromatin,NHChrom,centrosome,distal_app,ribosomes}; do
		IFS=','
		read -ra pathArray <<< "$(grep -i path, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
		read -ra setupAndIteration <<< "$(grep ,${dataset}, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
		correctPath="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/${setupAndIteration[0]}/${pathArray[1]}${setupAndIteration[2]}.n5/"
		actualPath="$(readlink /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/$cell.n5/$dataset)"
		if [[ "$actualPath" != *"$correctPath"* ]]; then
			echo "$cell $dataset"
			echo "correct: $correctPath"
			echo "actual: $actualPath"
			echo -e "\n"
		fi
	done
done

echo -e "\n\n\n"

for cell in {HeLa2,HeLa3,Jurkat,Macrophage}; do
        for dataset in {plasma_membrane,mito,mito_membrane,vesicle,MVB,er,microtubules,nucleus,golgi,ribosomes}; do
                IFS=','
                read -ra pathArray <<< "$(grep -i path, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
                read -ra setupAndIteration <<< "$(grep ,${dataset}, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
                correctPath="/nrs/cosem/cosem/training/v0003.2/${setupAndIteration[0]}/${pathArray[1]}${setupAndIteration[2]}.n5/"
                actualPath="$(readlink /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/rawPredictions/$cell.n5/$dataset)"
                if [[ "$actualPath" != *"$correctPath"* ]]; then
                        echo "$cell $dataset"
                        echo "correct: $correctPath"
                        echo "actual: $actualPath"
			echo -e "\n"
                fi
        done
done
