package org.janelia.saalfeldlab.hotknife;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class TestHelper {
    public static String testN5Locations = "src/test/resources/images.n5";
    public static String testFileLocations = "src/test/resources/analysis/";
    public static String tempN5Locations = "/tmp/test/images.n5/";
    public static String tempFileLocations = "/tmp/test/analysis/";
    public static int [] blockSize = {64,64,64};
    
    public static boolean validationAndTestN5sAreEqual(String dataset) throws IOException {
	 boolean areEqual = SparkCompareDatasets.setupSparkAndCompare(testN5Locations, tempN5Locations, dataset);
	 return areEqual;
    }
    
    public static boolean validationAndTestFilesAreEqual(String filename) throws IOException {
        File testFile = new File(testFileLocations + filename);
        File tempFile = new File(tempFileLocations + filename);
        boolean areEqual = FileUtils.contentEquals(testFile, tempFile);
        return areEqual;
    }
    
}
