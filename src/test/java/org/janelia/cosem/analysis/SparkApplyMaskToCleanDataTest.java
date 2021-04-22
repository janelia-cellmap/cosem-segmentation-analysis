package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.cosem.analysis.SparkApplyMaskToCleanData;
import org.janelia.saalfeldlab.n5.DataType;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.integer.UnsignedLongType;

public class SparkApplyMaskToCleanDataTest {

    @Test
    public void testMaskWithinBlock() {
	long [] dimensions = {10,10,10};
	Img<UnsignedLongType> halfFull = ImageCreationHelper.halfFull(dimensions, DataType.UINT64);

	int [][][] originalVoxelValues = new int [(int) dimensions[0]][(int) dimensions[1]][(int) dimensions[2]];

	for(int x=0; x<dimensions[0]; x++) 
	    for(int y=0; y<dimensions[1]; y++) 
		for(int z=0; z<dimensions[2]; z++) 
		     originalVoxelValues[x][y][z] = 1;
		
	
	Img<UnsignedLongType> originalImage = ImageCreationHelper.customImage(originalVoxelValues, DataType.UINT64);
	SparkApplyMaskToCleanData.maskWithinBlock(halfFull.randomAccess(), originalImage.randomAccess(), dimensions, true);
	assertTrue(ImageCreationHelper.compareDatasets(halfFull, originalImage));
	
    }

}
