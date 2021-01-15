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

import org.janelia.saalfeldlab.n5.DataType;

public class TestN5Maker {
    private static String path = "src/test/resources/images.n5";
    private static int [] blockSize = {5,5,5};

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
	TestImageMaker.writeCustomImage(path, "cylinderAndRectangle",voxelValues, blockSize, DataType.UINT8);
    }
    
    public static final void main(final String... args) throws IOException {
	createCylinderAndRectangleImage();
    }
}

