/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.janelia.saalfeldlab.n5.DataType;

public class TestN5Maker {

    public static void createCylinderAndRectangleImage() throws IOException {
	int [][][] voxelValues = new int [11][11][11];
	long [] dimensions = TestImageMaker.getDimensions(voxelValues);
	
	int radiusSquared = 16;
	int [] cylinderCenter = {5,5};
	for(int x=1; x<dimensions[0]-1; x++) {
	    for(int y=1; y<dimensions[1]-1; y++) {
		for(int z=1; z<dimensions[2]-1; z++) {
		    int deltaX = (x-cylinderCenter[0]);
		    int deltaY = (y-cylinderCenter[1]);
		    if(deltaX*deltaX + deltaY*deltaY <= radiusSquared && z<4) voxelValues[x][y][z] = 255; //cylinder
		    else if(z>7) voxelValues[x][y][z] = 255; //rectangular plane
		    else if(x==5 && y ==5) voxelValues[x][y][z] = 1; //thin line connecting them that should be below threshold
		}
	    }
	}
	TestImageMaker.writeCustomImage(TestConstants.testFileLocations, "cylinderAndRectangle",voxelValues, TestConstants.blockSize, DataType.UINT8);
    }
    
    public static void createTwoPlanesImage() throws IOException {
   	int [][][] voxelValues = new int [11][11][11];
   	long [] dimensions = TestImageMaker.getDimensions(voxelValues);
   	
   	for(int x=0; x<11; x+=10) {
   	    for(int y=1; y<dimensions[1]-1; y++) {
   		for(int z=1; z<dimensions[2]-1; z++) {
   		    if(z==0) {voxelValues[x][y][z] = 1;} //plane 1
   		    else {voxelValues[x][y][z] = 2;} //plane 2
   		}
   	    }
   	}
   	TestImageMaker.writeCustomImage(TestConstants.testFileLocations, "twoPlanes",voxelValues, TestConstants.blockSize, DataType.UINT8); //create it as already connected components
       }
    
    public static final void main(final String... args) throws IOException {
	//create basic test dataset and do connected components for it
	createCylinderAndRectangleImage();
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("cylinderAndRectangle", TestConstants.testFileLocations, null, TestConstants.testFileLocations, "_cc", 0, 1, false, false);
    
	//create additional dataset for contact site testing, default as connected components
	createTwoPlanesImage();
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("twoPlanes", TestConstants.testFileLocations, null, TestConstants.testFileLocations, "_cc", 0, 1, false, false);

	//do contact sites between the cylinderAndRectangle and twoPlanes datasets
	SparkContactSites.setupSparkAndCalculateContactSites(TestConstants.testFileLocations, TestConstants.testFileLocations, "cylinderAndRectangle_cc,twoPlanes_cc", null, 10, 1, false,false,false);

    }
}

