# COSEM Segmentation Analysis
Code to segment and analyze COSEM predictions.

## Installation

<details><summary> Command line </summary>
<ol>
<li> Clone repository and cd to the repository directory. </li>
<li> Run 
 
 `mvn compile`. </li>
<li> Once completed, you can run 
 
 `mvn package -Dmaven.test.skip=true`,
 
 the latter argument for skipping unit tests. However, if you plan on modifying the code and/or would like to run tests, we recommend the following: 
 
 `mvn package -Dspark.master=local[32] -DargLine="-Xmx100g"`. 
 
 The latter arguments are to ensure local spark is used for testing with enough memory. </li>
 </ol>
</details>

OR

<details><summary> Use an IDE such as Eclipse IDE </summary>
<ol>
<li> Clone the repository. </li>
<li> In Eclipse IDE, select File->Import->Existing Maven project and select the "cosem-segmentation-analysis" directory. </li>
<li> Right click on 
 
 `cosem-segmentation-analysis` in the project explorer and select `Run As` -> `Maven Build`, click `Skip Tests` checkbox if desired, and click `Run`. However, if you plan on modifying the code and/or would like to run tests, we recommend the following:  After selecting `Maven Build` as above, add the following parameter names and values:`spark.master`:`local[32]` and `argLine`:`-Xmx100g`, and click run. </li>
</ol>
</details>

To run one of the codes, eg. locally run SparkCompareDatasets, you can do the following (assuming spark is installed):
```bash 
/path/to/spark-submit --master local[*] --conf "spark.executor.memory=100g" --conf "spark.driver.memory=100g" --class org.janelia.cosem.analysis.SparkCurvature target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar --inputN5Path '/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis/src/test/resources/images.n5' --outputN5Path '/tmp/test/images.n5' --inputN5DatasetName 'shapes_cc'
```
