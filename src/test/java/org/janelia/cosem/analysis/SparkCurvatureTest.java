package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkCurvature;
import org.junit.Test;

public class SparkCurvatureTest {
    
    @Test
    public void testCurvature() throws Exception {
	SparkCurvature.setupSparkAndCalculateCurvature(TestHelper.testN5Locations, "shapes_cc", TestHelper.tempN5Locations, 12, false);
	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_sheetness"));
    }

}
