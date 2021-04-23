# COSEM Segmentation Analysis
Code to segment and analyze COSEM predictions. Requires [Maven](https://maven.apache.org/install.html), [Java JDK 8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html), and [Apache-Spark](https://spark.apache.org/downloads.html).

## Installation

Clone the repository `git clone https://github.com/davidackerman/cosem-segmentation-analysis` and cd to the repository directory.

<details><summary> Command line </summary>
<ol>
<li> Run 
 
 `mvn compile`. </li>
<li> Once completed, you can run 
 
 `mvn package -Dmaven.test.skip=true`,
 
 the latter argument for skipping unit tests. However, if you plan on modifying the code and/or would like to run tests, we recommend the following: 
 
 `mvn package -Dspark.master=local[*] -DargLine="-Xmx100g"`. 
 
 The latter arguments are to ensure local spark is used for testing with enough memory. </li>
 </ol>
</details>

OR

<details><summary> Use an IDE such as Eclipse IDE </summary>
<ol>
<li> In Eclipse IDE, select File->Import->Existing Maven project and select the "cosem-segmentation-analysis" directory. </li>
<li> Right click on 
 
 `cosem-segmentation-analysis` in the project explorer and select `Run As` -> `Maven Build`, click `Skip Tests` checkbox if desired, and click `Run`. However, if you plan on modifying the code and/or would like to run tests, we recommend the following:  After selecting `Maven Build` as above, add the following parameter names and values:`spark.master`:`local[32]` and `argLine`:`-Xmx100g`, and click run. </li>
</ol>
</details>

You will now have a compiled jar file located at `/path/to/cosem-segmentation-analysis/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar`.

To run one of the refinements, eg. locally run SparkConnectedComponents, you can do the following:
```bash 
/path/to/spark-submit --master local[*] --conf "spark.executor.memory=100g" --conf "spark.driver.memory=100g" \
--class org.janelia.cosem.analysis.SparkConnectedComponents \
/path/to/compiled/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar \
--inputN5Path '/path/to/input.n5' \
--outputN5Path '/path/to/output.n5' \
--inputN5DatasetName 'datasetName' \
--minimumVolumeCutoff '300E3' \
--thresholdIntensityCutoff 127 \
--outputN5DatasetSuffix '_cc'
```

Which will take the uint8 image `/path/to/input.n5/datsetName`, threshold at `127`, perform connected component analysis on the thresholded data applying a minimum volume cutoff of `300E3 nm^3` and writes the results to `/path/to/output.n5/datsetName_cc`.

To run another analysis, replace SparkConnectedComponents with the appropriate name and modify/add the requisite command line arguments.
