package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkTopologicalThinning;
import org.junit.Test;

public class SparkTopologicalThinningTest {
    
    @Test
    public void testConnectedComponents() throws Exception {
	SparkTopologicalThinning.setupSparkAndDoTopologicalThinning(TestHelper.testN5Locations, TestHelper.tempN5Locations, "shapes_cc", "_skeleton", false);
	SparkTopologicalThinning.setupSparkAndDoTopologicalThinning(TestHelper.testN5Locations, TestHelper.tempN5Locations, "shapes_cc", "_medialSurface", true);

	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_skeleton"));
	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_medialSurface"));
    }

}
