package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkCalculatePropertiesFromMedialSurface;
import org.junit.Test;

public class SparkCalculatePropertiesFromMedialSurfaceTest {
    
    @Test
    public void testCalculatePropertiesFromMedialSurface() throws Exception {
	SparkCalculatePropertiesFromMedialSurface.setupSparkAndCalculatePropertiesFromMedialSurface(TestHelper.testN5Locations, "shapes_cc", TestHelper.tempN5Locations, TestHelper.tempFileLocations, false);
	
	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_sheetnessVolumeAveraged"));
	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc_sheetnessVolumeAndAreaHistograms.csv"));
	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc_sheetnessVsThicknessHistogram.csv"));
    }

}
