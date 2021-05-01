package org.janelia.cosem.util;
import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDirectoryDelete{
	
	public static void deleteDirectories(final JavaSparkContext sparkContext, final List<String> listOfDirectories) {
		final JavaRDD<String> rdd = sparkContext.parallelize(listOfDirectories);
		rdd.foreach(currentDirectory -> {
			FileUtils.deleteDirectory(new File(currentDirectory));
		});
	}
	
	public static void deleteDirectories(final SparkConf conf, final List<String> listOfDirectories) {
		final JavaSparkContext sparkContext = new JavaSparkContext(conf);
		deleteDirectories(sparkContext, listOfDirectories);
		sparkContext.close();
	}
}