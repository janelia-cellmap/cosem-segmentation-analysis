package org.janelia.cosem.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.janelia.cosem.analysis.SparkCompareDatasets;

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
    
    public static int getNumberOfLines(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        int lines = 0;
        while (reader.readLine() != null) lines++;
        reader.close();
        
        return lines;
    }
    
    public static String removeExcessCharacters(String str) {
	if (str.contains(",,")) {//then this is an annoying thing to have to compare so dont care about part after it
	    str = str.split(",,")[0]+",";
	}
	return str;
    }
    
    public static boolean equalButDifferentLineOrder(String testFilename, String tempFilename) throws IOException {

	if( getNumberOfLines(testFilename)==getNumberOfLines(tempFilename) ){
	    File testFile = new File(testFilename);
	    File tempFile = new File(tempFilename);
	    
	    Scanner input1= new Scanner(testFile);
            Scanner input2= new Scanner(tempFile);
            
            Set<String> set1 = new HashSet<String>();
            Set<String> set2 = new HashSet<String>();
            
            while(input1.hasNext()) {
        	String str1 = removeExcessCharacters( input1.nextLine() );
        	String str2 = removeExcessCharacters( input2.nextLine() );        	
        	set1.add(str1);
            	set2.add(str2);
            }
         
            input1.close();
            input2.close();
            return set1.equals(set2);  
	}
	return false;
    }
    
    public static boolean validationAndTestFilesAreEqual(String filename) throws IOException {
	String testFilename = testFileLocations + filename;
	String tempFilename = tempFileLocations + filename;
        File testFile = new File(testFilename);
        File tempFile = new File(tempFilename);
        boolean areEqual = FileUtils.contentEquals(testFile, tempFile);
        
        if(!areEqual) { //then may be just out of order
            return equalButDifferentLineOrder(testFilename, tempFilename);
        }
        return areEqual;
    }
    
}
