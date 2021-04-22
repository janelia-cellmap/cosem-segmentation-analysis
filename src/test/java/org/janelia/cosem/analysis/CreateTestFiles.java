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
package org.janelia.cosem.analysis;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkGetRenumbering;
import org.janelia.cosem.analysis.SparkRenumberN5;
import org.janelia.saalfeldlab.n5.DataType;

public class CreateTestFiles {
    
    public static void createShapesImage() throws IOException {
	int [][][] voxelValues = new int [150][150][150];
	long [] dimensions = ImageCreationHelper.getDimensions(voxelValues);
	
	//cylinder
	double cylinderRadiusSquared = 49*49;
	int [] cylinderCenter = {75,50};
	
	//sphere
	double sphereRadiusSquared = 29*29;
	int [] sphereCenter = {117,117,98};
	
	//slab
	int slabStart = 140;
	
	
	for(int x=1; x<dimensions[0]-1; x++) {
	    for(int y=1; y<dimensions[1]-1; y++) {
		for(int z=1; z<dimensions[2]-1; z++) {
		    int deltaXCylinder = (x-cylinderCenter[0]);
		    int deltaYCylinder = (y-cylinderCenter[1]);
		    
		    int deltaXSphere = (x-sphereCenter[0]);
		    int deltaYSphere = (y-sphereCenter[1]);
		    int deltaZSphere = (z-sphereCenter[2]);
		    
		    if(deltaXCylinder*deltaXCylinder + deltaYCylinder*deltaYCylinder <= cylinderRadiusSquared && z<slabStart-2) voxelValues[x][y][z] = 255; //cylinder
		    else if(deltaXSphere*deltaXSphere + deltaYSphere*deltaYSphere + deltaZSphere*deltaZSphere <= sphereRadiusSquared) 
			{	
				if(x>sphereCenter[0]-5 && x<sphereCenter[0]+5 && y>sphereCenter[1]-5 && y<sphereCenter[1]+5 && z>sphereCenter[2]-5 && z<sphereCenter[2]+5)
				{
				    voxelValues[x][y][z] = 0; //hole	
				}
				else {
				    voxelValues[x][y][z] = 255; //sphere	
				}
			}
		    else if(z>slabStart) voxelValues[x][y][z] = 255; //rectangular plane
		    else voxelValues[x][y][z] = 126; //should be below threshold
		    
		    //U shape
		    if(x>130 && x<140 && z>70 && z<80 && y<4) {
			voxelValues[x][y][z]=255;
			if(y==2 && x<139) {
				voxelValues[x][y][z]=0;
			}
		    }
		    
		    //3 small chunks, 2 of which should be removed
		    voxelValues[100][98][75]=255;
		    voxelValues[100][99][75]=255;
		    voxelValues[100][100][75]=255;
		    
		    voxelValues[102][98][75]=255;
		    voxelValues[102][99][75]=255;
		    
		    voxelValues[104][99][75]=255;
		   
		    

		}
	    }
	}
	ImageCreationHelper.writeCustomImage(TestHelper.testN5Locations, "shapes",voxelValues, TestHelper.blockSize, DataType.UINT8);
    }
    
    public static void createShapesForCurvatureImage() throws IOException {
   	int [][][] voxelValues = new int [150][150][150];
   	long [] dimensions = ImageCreationHelper.getDimensions(voxelValues);
   	
   	//cylinder
   	double cylinderRadiusSquared = 49*49;
   	int [] cylinderCenter = {75,50};
   	
   	//sphere
   	double sphereRadiusSquared = 29*29;
   	int [] sphereCenter = {117,117,98};
   	
   	//slab
   	int slabStart = 140;
   	
   	
   	for(int x=1; x<dimensions[0]-1; x++) {
   	    for(int y=1; y<dimensions[1]-1; y++) {
   		for(int z=1; z<dimensions[2]-1; z++) {
   		    int deltaXCylinder = (x-cylinderCenter[0]);
   		    int deltaYCylinder = (y-cylinderCenter[1]);
   		    
   		    int deltaXSphere = (x-sphereCenter[0]);
   		    int deltaYSphere = (y-sphereCenter[1]);
   		    int deltaZSphere = (z-sphereCenter[2]);
   		    
   		    if(deltaXCylinder*deltaXCylinder + deltaYCylinder*deltaYCylinder <= cylinderRadiusSquared && z<slabStart-2) voxelValues[x][y][z] = 255; //cylinder
   		    else if(deltaXSphere*deltaXSphere + deltaYSphere*deltaYSphere + deltaZSphere*deltaZSphere <= sphereRadiusSquared) 
   			{	
   				    voxelValues[x][y][z] = 255; //sphere	
   			}
   		    else if(z>slabStart) voxelValues[x][y][z] = 255; //rectangular plane
   		    else voxelValues[x][y][z] = 0; //should be below threshold
   		}
   	    }
   	}
   	ImageCreationHelper.writeCustomImage(TestHelper.testN5Locations, "shapesForCurvature",voxelValues, TestHelper.blockSize, DataType.UINT8);
       }
    
    public static void createSaddleShapeImage() throws IOException {
	
	int [][][] voxelValues = new int [150][150][150];
	long [] dimensions = ImageCreationHelper.getDimensions(voxelValues);
	int centerX = 75, centerY=75, zCenter=25;
	for(int x=1; x<dimensions[0]-1; x++) {
	    for(int y=1; y<dimensions[1]-1; y++) {
		for(int z=1; z<dimensions[2]-1; z++) {
		    double zSaddle = zCenter+(1/50.0)*Math.pow(x-centerX,2)-(1/100.0)*Math.pow(y-centerY,2);
		    if (z<=zSaddle) {
			voxelValues[x][y][z] = 1;
		    }
		}
	    }
	}
	ImageCreationHelper.writeCustomImage(TestHelper.testN5Locations, "saddleShape",voxelValues, TestHelper.blockSize, DataType.UINT8);
    }
    
    public static void createPlanesImage() throws IOException {
   	int [][][] voxelValues = new int [150][150][150];
   	long [] dimensions = ImageCreationHelper.getDimensions(voxelValues);
   	
   	for(int x=0; x<dimensions[0]; x++) {
   	    for(int y=0; y<dimensions[1]; y++) {
   		for(int z=0; z<dimensions[2]; z++) {
   		    if((x==0 || x == dimensions[0]-1) ^ (y==0 || y==dimensions[1]-1) ^ (z==0 || z==dimensions[2]-1)) {
   			voxelValues[x][y][z] = 255;
   		    } 
   		}
   	    }
   	}
   	
   	ImageCreationHelper.writeCustomImage(TestHelper.testN5Locations, "planes",voxelValues, TestHelper.blockSize, DataType.UINT8); //create it as already connected components
       }
    
    public static final void main(final String... args) throws Exception {
	//create basic test dataset and do connected components for it
	createShapesImage();
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("shapes", TestHelper.testN5Locations, null, TestHelper.testN5Locations, "_cc", 0, 1, false, false);
	SparkFillHolesInConnectedComponents.setupSparkAndFillHolesInConnectedComponents(TestHelper.testN5Locations, TestHelper.testN5Locations, "shapes_cc", 0, "_filled", false, false);

	//create additional dataset for contact site testing, default as connected components
	createPlanesImage();
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("planes", TestHelper.testN5Locations, null, TestHelper.testN5Locations, "_cc", 0, 1, false, false);
	createShapesForCurvatureImage();
	createSaddleShapeImage();
	
	//do contact sites between the cylinderAndRectangle and twoPlanes datasets
	SparkContactSites.setupSparkAndCalculateContactSites(TestHelper.testN5Locations, TestHelper.testN5Locations, "shapes_cc,planes_cc", null, 10, 1, false,false,false);

	//topological thinning: skeletonization and medial surface
	SparkTopologicalThinning.setupSparkAndDoTopologicalThinning(TestHelper.testN5Locations, TestHelper.testN5Locations, "shapes_cc", "_skeleton", false);
	SparkTopologicalThinning.setupSparkAndDoTopologicalThinning(TestHelper.testN5Locations, TestHelper.testN5Locations, "shapes_cc", "_medialSurface", true);
    
	//calculate curvature of dataset
	SparkCurvature.setupSparkAndCalculateCurvature(TestHelper.testN5Locations, "shapes_cc", TestHelper.testN5Locations, 12, false);
	
	//calculate properties from medial surface
	SparkCalculatePropertiesFromMedialSurface.setupSparkAndCalculatePropertiesFromMedialSurface(TestHelper.testN5Locations, "shapes_cc", TestHelper.testN5Locations, TestHelper.testFileLocations, false);
  
	//sheetness of contact sites
	SparkCalculateSheetnessOfContactSites.setupSparkAndCalculateSheetnessOfContactSites(TestHelper.testN5Locations, "shapes_cc_sheetnessVolumeAveraged", TestHelper.testFileLocations, "shapes_cc_to_planes_cc_cc");
   
	//general information output
	SparkGeneralCosemObjectInformation.setupSparkAndRunGeneralCosemObjectInformation("shapes_cc", TestHelper.testN5Locations, "shapes_cc_to_planes_cc", TestHelper.testFileLocations, true, true);
	SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow("shapes", TestHelper.testN5Locations, null, TestHelper.testN5Locations, "_standard", 0, 1, false, false);

	//renumbering files
	SparkGetRenumbering.setupSparkAndGetRenumbering(TestHelper.testN5Locations, TestHelper.testN5Locations,TestHelper.testFileLocations, "shapes_cc",null);
	SparkRenumberN5.setupSparkAndRenumberN5(TestHelper.testFileLocations, "shapes_cc", TestHelper.testN5Locations, TestHelper.testN5Locations, "shapes_cc");
	SparkGetRenumbering.setupSparkAndGetRenumbering(TestHelper.testN5Locations, TestHelper.testN5Locations,TestHelper.testFileLocations, "shapes_cc_medialSurface","shapes_cc_renumbered");

    }
}

