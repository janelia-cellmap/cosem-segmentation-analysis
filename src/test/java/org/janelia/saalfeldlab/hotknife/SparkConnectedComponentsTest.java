package org.janelia.saalfeldlab.hotknife;

import static org.junit.Assert.*;

import java.io.IOException;
import org.junit.Test;

public class SparkConnectedComponentsTest {
    
    @Test
    public void testConnectedComponents() throws IOException {
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("cylinderAndRectangle", TestConstants.testFileLocations, null, TestConstants.tempFileLocations, "_cc", 0, 1, false, false);	
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("twoPlanes", TestConstants.testFileLocations, null, TestConstants.tempFileLocations, "_cc", 0, 1, false, false);

	boolean areEqual = SparkCompareDatasets.setupSparkAndCompare(TestConstants.testFileLocations, TestConstants.tempFileLocations, "cylinderAndRectangle_cc");
	assertTrue(areEqual);
	
	areEqual = SparkCompareDatasets.setupSparkAndCompare(TestConstants.testFileLocations, TestConstants.tempFileLocations, "twoPlanes_cc");
	assertTrue(areEqual);
    }

}
