package org.janelia.cosem.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Skeletonize3D plugin for ImageJ(C).
 * Copyright (C) 2008 Ignacio Arganda-Carreras 
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation (http://www.gnu.org/licenses/gpl.txt )
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 * 
 */

import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.plugin.filter.PlugInFilter;
import ij.process.ImageProcessor;
import net.imglib2.RandomAccess;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.view.IntervalView;


/**
 * Main class.
 * This class is a plugin for the ImageJ interface for 2D and 3D thinning 
 * (skeletonization) of binary images (2D/3D).
 *
 * <p>
 * This work is an implementation by Ignacio Arganda-Carreras of the
 * 3D thinning algorithm from Lee et al. "Building skeleton models via 3-D 
 * medial surface/axis thinning algorithms. Computer Vision, Graphics, and 
 * Image Processing, 56(6):462–478, 1994." Based on the ITK version from
 * Hanno Homann <a href="http://hdl.handle.net/1926/1292"> http://hdl.handle.net/1926/1292</a>
 * <p>
 *  More information at Skeletonize3D homepage:
 *  http://imagejdocu.tudor.lu/doku.php?id=plugin:morphology:skeletonize3d:start
 *
 * @version 1.0 11/19/2008
 * @author Ignacio Arganda-Carreras (iargandacarreras at gmail.com)
 *
 * Updated by:
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class Skeletonize3D_ implements PlugInFilter 
{

	/** working image width */
	private int width = 0;
	/** working image height */
	private int height = 0;
	/** working image depth */
	private int depth = 0;
	/** working image stack*/
	private IntervalView<UnsignedByteType> inputImage = null;
	
	/** number of iterations thinning took */
	private int iterations;
	private int[][] padding;
	private int[] paddedOffset;
	
	private int currentBorder;
	
	private int[] eulerLUT;
	private int[] pointsLUT;
	
	public Skeletonize3D_(IntervalView<UnsignedByteType> inputImage, int[][] padding, long[] paddedOffset) {
		this.inputImage = inputImage;
		this.width = (int) this.inputImage.dimension(0);
		this.height = (int) this.inputImage.dimension(1);
		this.depth = (int) this.inputImage.dimension(2);
		this.padding = padding; 
		
		//just in case it's negative padding
		this.paddedOffset = new int[] {(int) paddedOffset[0], (int) paddedOffset[1], (int)paddedOffset[2]};
		this.paddedOffset[0]=Math.abs(this.paddedOffset[0]);
		this.paddedOffset[1]=Math.abs(this.paddedOffset[1]);
		this.paddedOffset[2]=Math.abs(this.paddedOffset[2]);
		
		// Prepare Euler LUT [Lee94]
		this.eulerLUT = new int[256]; 
		fillEulerLUT( this.eulerLUT );
		
		// Prepare number of points LUT
		this.pointsLUT = new int[ 256 ];
		fillnumOfPointsLUT(this.pointsLUT);
	}

	
	/**
	 * Performs one thinning iteration for skeletonization
	 * 
	 */
	public boolean computeSkeletonIteration() 
	{	
		RandomAccess<UnsignedByteType> outputImageRandomAccess = inputImage.randomAccess();
		
		iterations = 0;
		// Loop through the image several times until there is no change.
		iterations++;
		boolean needToThinAgain = false;				
		for(int currentBorder = 0; currentBorder<6; currentBorder++) 
		{
			// Following Lee[94], save versions (Q) of input image S, while 
			// deleting each type of border points (R)
			ArrayList<ArrayList<int[]>> simpleBorderPoints = new ArrayList<ArrayList<int[]>>();
			for(int i=0; i<8; i++) {
				simpleBorderPoints.add(new ArrayList<int[]>());
			}
			// Loop through the image.
			for (int z = 1; z < depth-1; z++)
			{
				for (int y = 1; y < height-1; y++)
				{
					for (int x = 1; x < width-1; x++)						
					{
						// check if point is foreground
						if ( getPixelNoCheck(outputImageRandomAccess, x, y, z) == 0 )
						{
							continue;         // current point is already background 
						}
												
						// check 6-neighbors if point is a border point of type currentBorder
						boolean isBorderPoint = false;
						// North
						if( currentBorder == 2 && N(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						// South
						if( currentBorder == 3 && S(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						// East
						if( currentBorder == 5 && E(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						// West
						if( currentBorder == 4 && W(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						if(inputImage.dimension(2) > 0)
						{
							// Up							
							if( currentBorder == 0 && U(outputImageRandomAccess, x, y, z) <= 0 )
								isBorderPoint = true;
							// Bottom
							if( currentBorder == 1 && B(outputImageRandomAccess, x, y, z) <= 0 )
								isBorderPoint = true;
						}
						if( !isBorderPoint )
						{
							continue;         // current point is not deletable
						}
						
						if( isEndPoint( outputImageRandomAccess, x, y, z))
						{	
							continue;
						}
						
						final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, x, y, z);
						
						// Check if point is Euler invariant (condition 1 in Lee[94])
						if( !isEulerInvariant( neighborhood, eulerLUT ) )
						{   
							continue;         // current point is not deletable
						}
						
						// Check if point is simple (deletion does not change connectivity in the 3x3x3 neighborhood)
						// (conditions 2 and 3 in Lee[94])
						if( !isSimplePoint( neighborhood ) )
						{   
							continue;         // current point is not deletable
						}
	
	
						// add all simple border points to a list for sequential re-checking, based on overall position in image
						int[] index = new int[3];
						index[0] = x;
						index[1] = y;
						index[2] = z;
						simpleBorderPoints.get((x+paddedOffset[0])%2+((y+paddedOffset[1])%2)*2+((z+paddedOffset[2])%2)*4).add(index);
					}
				}					
			}							
			
			for (ArrayList<int[]> subvolumeSimpleBorderPoints : simpleBorderPoints) {
				for (int[] index : subvolumeSimpleBorderPoints) {			
					final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, index[0], index[1], index[2]);
					final byte[] neighborhoodIfRemoved = neighborhood;
					neighborhoodIfRemoved[13]=0;
					//Do actual rechecking, ie, making sure that if point is removed, everything is still satisfied
					// Check if border points is simple
					if (isSimplePoint(neighborhood) && isEulerInvariant( neighborhood, eulerLUT ) &&
							isSimplePoint(neighborhoodIfRemoved) && isEulerInvariant( neighborhoodIfRemoved, eulerLUT ))//condition 4 in paper
							{						// we can delete the current point
						setPixel(outputImageRandomAccess, index[0], index[1], index[2], (byte) 0);
						needToThinAgain |= true;
					}
				}
			}
		
		}
		return needToThinAgain;
	} 	
	
	
	/**
	 * Performs one thinning iteration for medial surface thinning
	 * 
	 */
	public boolean computeMedialSurfaceIteration() 
	{
		RandomAccess<UnsignedByteType> outputImageRandomAccess = inputImage.randomAccess();

		// Following Lee[94], save versions (Q) of input image S, while 
		// deleting each type of border points (R)
		
		
		boolean needToThinAgain = false;
		for(currentBorder = 0; currentBorder<6; currentBorder++) {
			ArrayList<ArrayList<int[]>> simpleBorderPoints = new ArrayList<ArrayList<int[]>>();
			for(int i=0; i<8; i++) {
				simpleBorderPoints.add(new ArrayList<int[]>());
			}		
			// Loop through the image.				 
			for (int z = 1; z < depth-1; z++)
			{
				for (int y = 1; y < height-1; y++)
				{
					for (int x = 1; x < width-1; x++)						
					{

						// check if point is foreground
						if ( getPixelNoCheck(outputImageRandomAccess, x, y, z) == 0 )
						{
							continue;         // current point is already background 
						}
																			
						// check 6-neighbors if point is a border point of type currentBorder
						boolean isBorderPoint = false;
						// North
						if( currentBorder == 2 && N(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						// South
						if( currentBorder == 3 && S(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						// East
						if( currentBorder == 5 && E(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						// West
						if( currentBorder == 4 && W(outputImageRandomAccess, x, y, z) <= 0 )
							isBorderPoint = true;
						if(inputImage.dimension(2) > 0)
						{
							// Up							
							if( currentBorder == 0 && U(outputImageRandomAccess, x, y, z) <= 0 )
								isBorderPoint = true;
							// Bottom
							if( currentBorder == 1 && B(outputImageRandomAccess, x, y, z) <= 0 )
								isBorderPoint = true;
						}
						if( !isBorderPoint )
						{
							continue;         // current point is not deletable
						}
						
						final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, x, y, z);

						if(isSurfaceEndPoint(neighborhood))
						{ //check it again anyway but just saves some time
							continue;
						}

						
						// Check if point is Euler invariant (condition 1 in Lee[94])
						if( !isEulerInvariant( neighborhood, eulerLUT ) )
						{
							continue;         // current point is not deletable
						}

						// Check if point is simple (deletion does not change connectivity in the 3x3x3 neighborhood)
						// (conditions 2 and 3 in Lee[94])
						if( !isSimplePoint( neighborhood ) )
						{
							continue;         // current point is not deletable
						}

						// add all simple border points to a list for sequential re-checking, based on overall position in image
						int[] index = new int[3];
						index[0] = x;
						index[1] = y;
						index[2] = z;
						
						simpleBorderPoints.get((x+paddedOffset[0])%2+((y+paddedOffset[1])%2)*2+((z+paddedOffset[2])%2)*4).add(index);
					}
				}					
			}							


			// sequential re-checking to preserve connectivity when
			// deleting in a parallel way
			for (ArrayList<int[]> subvolumeSimpleBorderPoints : simpleBorderPoints) {
				for (int[] index : subvolumeSimpleBorderPoints) {			
					final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, index[0], index[1], index[2]);
					final byte[] neighborhoodIfRemoved = neighborhood;
					neighborhoodIfRemoved[13]=0;
					// Check if border points is simple
					if (isSimplePoint(neighborhood) && isEulerInvariant( neighborhood, eulerLUT ) &&
							isSimplePoint(neighborhoodIfRemoved) && isEulerInvariant( neighborhoodIfRemoved, eulerLUT ))//condition 4 in paper
							{						// we can delete the current point
						setPixel(outputImageRandomAccess, index[0], index[1], index[2], (byte) 0);
						needToThinAgain |= true;
					}
				}
			}
		}
		return needToThinAgain;	
	}
	
	/**
	 * Check if voxel location is a border voxel
	 * 
	 * @param outputImageRandomAccess - image
	 * @param x		X coordinate
	 * @param y		Y coordinate
	 * @param z		Z coordinate
	 * @return 		true if voxel is a border
	 */
	public boolean isAnyBorder(RandomAccess<UnsignedByteType> outputImageRandomAccess, int x, int y, int z) {
		return (U(outputImageRandomAccess,x,y,z)<=0 || B(outputImageRandomAccess,x,y,z)<=0 || N(outputImageRandomAccess,x,y,z)<=0 || S(outputImageRandomAccess,x,y,z)<=0 || W(outputImageRandomAccess,x,y,z)<=0 || E(outputImageRandomAccess,x,y,z)<=0);
	}
	
	/**
	 * Check if the block is independent while doing medial surface thinning meaning that thinning within it wont be affected by other blocks any more
	 * 
	 * @return 		true if medial surface block is independent of others
	 */
	public boolean isMedialSurfaceBlockIndependent() {
		RandomAccess<UnsignedByteType> outputImageRandomAccess = inputImage.randomAccess();	
		
		int[] paddingNeg = padding[0];
		int[] paddingPos = padding[1];
		List<Integer> xEdges = Arrays.asList(paddingNeg[0], width-paddingPos[0]-1);
		List<Integer> yEdges = Arrays.asList(paddingNeg[1], height-paddingPos[1]-1);
		List<Integer> zEdges = Arrays.asList(paddingNeg[2], depth-paddingPos[2]-1);
		for(int x : xEdges) {
			for(int y=yEdges.get(0); y<=yEdges.get(1); y++) {
				for(int z=zEdges.get(0); z<=zEdges.get(1); z++) {
					if (getPixelNoCheck(outputImageRandomAccess,x,y,z)==0) continue; //Background
					if (!isAnyBorder(outputImageRandomAccess,x,y,z)) return false; //Then it is not a border point, so unclear whether it is finished
					final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, x, y, z);
					if(!(isSurfaceEndPoint(neighborhood))) return false; //Then it is not a surface point, so isn't done. Surface endpoints can't revert from being surface endpoints? But simple/euler invariant cant switch?
				}
			}
		}
		
		for(int y : yEdges) {
			for(int x=xEdges.get(0); x<=xEdges.get(1); x++) {
				for(int z=zEdges.get(0); z<=zEdges.get(1); z++) {
					if (getPixelNoCheck(outputImageRandomAccess,x,y,z)==0) continue; //Background
					if (!isAnyBorder(outputImageRandomAccess,x,y,z)) return false; //Then it is not a border point, so unclear whether it is finished
					final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, x, y, z);
					if(!(isSurfaceEndPoint(neighborhood))) return false; //Then it is not a surface point, so isn't done. Surface endpoints can't revert from being surface endpoints? But simple/euler invariant cant switch?
				}
			}
		}
		
		for(int z : zEdges) {
			for(int x=xEdges.get(0); x<=xEdges.get(1); x++) {
				for(int y=yEdges.get(0); y<=yEdges.get(1); y++) {
					if (getPixelNoCheck(outputImageRandomAccess,x,y,z)==0) continue; //Background
					if (!isAnyBorder(outputImageRandomAccess,x,y,z)) return false; //Then it is not a border point, so unclear whether it is finished
					final byte[] neighborhood = getNeighborhood(outputImageRandomAccess, x, y, z);
					if(!(isSurfaceEndPoint(neighborhood))) return false; //Then it is not a surface point, so isn't done. Surface endpoints can't revert from being surface endpoints? But simple/euler invariant cant switch?
				}
			}
		}
		 
		//If it made it all the way through, then all points on edge of block are done being thinned so it is independent
		return true;
	}
	
	/**
	 * Check if skeleton block is indepenedent meaning that thinning within it wont be affected by other blocks any more
	 * @return 		true if skeleton block is independent of other blocks
	 */
	public boolean isSkeletonBlockIndependent() {
		RandomAccess<UnsignedByteType> outputImageRandomAccess = inputImage.randomAccess();		
		
		int[] paddingNeg = padding[0];
		int[] paddingPos = padding[1];
		List<Integer> xEdges = Arrays.asList(paddingNeg[0], width-paddingPos[0]-1);
		List<Integer> yEdges = Arrays.asList(paddingNeg[1], height-paddingPos[1]-1);
		List<Integer> zEdges = Arrays.asList(paddingNeg[2], depth-paddingPos[2]-1);
		for(int x : xEdges) {
			for(int y=yEdges.get(0); y<=yEdges.get(1); y++) {
				for(int z=zEdges.get(0); z<=zEdges.get(1); z++) {
					if (getPixelNoCheck(outputImageRandomAccess,x,y,z)==0) continue; //Background
					if (!isAnyBorder(outputImageRandomAccess,x,y,z)) return false; //Then it is not a border point, so unclear whether it is finished
					if(!(isEndPoint(outputImageRandomAccess, x, y, z) || isOneVoxelThinAndDone(outputImageRandomAccess, x, y, z))) return false; //Then it is not a surface point, so isn't done. Surface endpoints can't revert from being surface endpoints? But simple/euler invariant cant switch?
				}
			}
		}
		
		for(int y : yEdges) {
			for(int x=xEdges.get(0); x<=xEdges.get(1); x++) {
				for(int z=zEdges.get(0); z<=zEdges.get(1); z++) {
					if (getPixelNoCheck(outputImageRandomAccess,x,y,z)==0) continue; //Background
					if (!isAnyBorder(outputImageRandomAccess,x,y,z)) return false; //Then it is not a border point, so unclear whether it is finished
					if(!(isEndPoint(outputImageRandomAccess, x, y, z) || isOneVoxelThinAndDone(outputImageRandomAccess, x, y, z))) return false; //Then it is not a surface point, so isn't done. Surface endpoints can't revert from being surface endpoints? But simple/euler invariant cant switch?
				}
			}
		}
		
		for(int z : zEdges) {
			for(int x=xEdges.get(0); x<=xEdges.get(1); x++) {
				for(int y=yEdges.get(0); y<=yEdges.get(1); y++) {
					if (getPixelNoCheck(outputImageRandomAccess,x,y,z)==0) continue; //Background
					if (!isAnyBorder(outputImageRandomAccess,x,y,z)) return false; //Then it is not a border point, so unclear whether it is finished
					if(!(isEndPoint(outputImageRandomAccess, x, y, z) || isOneVoxelThinAndDone(outputImageRandomAccess, x, y, z))) return false; //Then it is not a surface point, so isn't done. Surface endpoints can't revert from being surface endpoints? But simple/euler invariant cant switch?
				}
			}
		}
		 
		//If it made it all the way through, then all points on edge of block are done being thinned so it is independent
		return true;
	}
	
	/**
	 * Skeleton thinning at this voxel is done
	 * 
	 * @param ra	Image random access
	 * @param x		X coordinate
	 * @param y		Y coordinate
	 * @param z		Z coordinate
	 * @return 		true if voxel location has been skeletonized as much as possible
	 */
	boolean isOneVoxelThinAndDone(RandomAccess<UnsignedByteType> ra, int x, int y, int z) {
		byte[] neighborhood = getNeighborhood(ra, x, y, z);
		if(numberOfNeighbors(neighborhood) == 2 && !isSimplePoint(neighborhood) && !isEulerInvariant(neighborhood, eulerLUT)) {
			return true;
		}
		return false;
	}
	
	/**
	 * Get number of voxel neighbors 
	 * 
	 * @param neighborhood		Neighborhood around voxel
	 * @return 					Number of non-background voxel neighbors
	 */
	int numberOfNeighbors(byte[] neighborhood) {
		int numberOfNeighbors = -1;
		for( int i = 0; i < 27; i++ ) // i =  0..26
	      {					        	
	        if( neighborhood[i] == 1 )
	          numberOfNeighbors++;
	      }
		return numberOfNeighbors;
	}
	
	/**
	 * Check if a voxel is a surface endpoint during medial surface thinning. Based on the paper
	 * 
	 * @param neighbors		Neighborhood around voxel
	 * @return 				true if is a surface endpoint
	 */
	boolean isSurfaceEndPoint(byte[] neighbors)
	{ //Definition 1 in paper
		
		List<Character> allowedIndexValues = Arrays.asList((char)(51), (char)(15), (char)(85), //non-diagonals
				(char)(195), (char)(165), (char)(153)); //diagonals; I think the paper is wrong here in which values it says are allowed
		// Octant SWU
		char indices [] = new char[8];
		indices[0] = indexOctantSWU(neighbors);
		indices[1] = indexOctantSEU(neighbors);
		indices[2] = indexOctantNWU(neighbors);
		indices[3] = indexOctantNEU(neighbors);
		indices[4] = indexOctantSWB(neighbors);
		indices[5] = indexOctantSEB(neighbors);
		indices[6] = indexOctantNWB(neighbors);
		indices[7] = indexOctantNEB(neighbors);
		for(int octant=0; octant<8; octant++) {
			boolean conditionA = allowedIndexValues.contains(indices[octant]);
			int numberOfPointsInOctant = Integer.bitCount((int) indices[octant])-1;//-1 to exclude v?
			boolean conditionB = numberOfPointsInOctant<3;
			if (! (conditionA || conditionB) ) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Bitwise or a pixel value
	 * @param ra	Image random access
	 * @param x		X position
	 * @param y		Y position
	 * @param z		Z position
	 * @param b		Byte to or with
	 */
	void bitwiseOrPixel(RandomAccess<UnsignedByteType> ra, int x, int y, int z, byte b){
		byte newValue = (byte) (getPixelByte(ra, x, y, z) | b);
		setPixel(ra, x, y, z, newValue);
	}
	
	/**
	 * Check if a point in the given stack is at the end of an arc
	 * 
	 * @param image	The stack of a 3D binary image
	 * @param x		The x-coordinate of the point
	 * @param y		The y-coordinate of the point
	 * @param z		The z-coordinate of the point (>= 1)
	 * @return		true if the point has exactly one neighbor
	 */
	boolean isEndPoint(ImageStack image, int x, int y, int z)
	{
		int numberOfNeighbors = -1;   // -1 and not 0 because the center pixel will be counted as well
        byte[] neighbor = getNeighborhood(image, x, y, z);
        for( int i = 0; i < 27; i++ ) // i =  0..26
        {					        	
          if( neighbor[i] == 1 )
            numberOfNeighbors++;
        }

        return  numberOfNeighbors == 1;        
	}
	
	boolean isEndPoint(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		int numberOfNeighbors = -1;   // -1 and not 0 because the center pixel will be counted as well
        byte[] neighbor = getNeighborhood(ra, x, y, z);
        for( int i = 0; i < 27; i++ ) // i =  0..26
        {					        	
          if( neighbor[i] == 1 )
            numberOfNeighbors++;
        }

        return  numberOfNeighbors == 1;        
	}
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Get neighborhood of a pixel in a 3D image (0 border conditions) 
	 * 
	 * @param image 3D image (ImageStack)
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding 27-pixels neighborhood (0 if out of image)
	 */
	public byte[] getNeighborhood(ImageStack image, int x, int y, int z)
	{
		byte[] neighborhood = new byte[27];
		
		neighborhood[ 0] = getPixel(image, x-1, y-1, z-1);
		neighborhood[ 1] = getPixel(image, x  , y-1, z-1);
		neighborhood[ 2] = getPixel(image, x+1, y-1, z-1);
		
		neighborhood[ 3] = getPixel(image, x-1, y,   z-1);
		neighborhood[ 4] = getPixel(image, x,   y,   z-1);
		neighborhood[ 5] = getPixel(image, x+1, y,   z-1);
		
		neighborhood[ 6] = getPixel(image, x-1, y+1, z-1);
		neighborhood[ 7] = getPixel(image, x,   y+1, z-1);
		neighborhood[ 8] = getPixel(image, x+1, y+1, z-1);
		
		neighborhood[ 9] = getPixel(image, x-1, y-1, z  );
		neighborhood[10] = getPixel(image, x,   y-1, z  );
		neighborhood[11] = getPixel(image, x+1, y-1, z  );
		
		neighborhood[12] = getPixel(image, x-1, y,   z  );
		neighborhood[13] = getPixel(image, x,   y,   z  );
		neighborhood[14] = getPixel(image, x+1, y,   z  );
		
		neighborhood[15] = getPixel(image, x-1, y+1, z  );
		neighborhood[16] = getPixel(image, x,   y+1, z  );
		neighborhood[17] = getPixel(image, x+1, y+1, z  );
		
		neighborhood[18] = getPixel(image, x-1, y-1, z+1);
		neighborhood[19] = getPixel(image, x,   y-1, z+1);
		neighborhood[20] = getPixel(image, x+1, y-1, z+1);
		
		neighborhood[21] = getPixel(image, x-1, y,   z+1);
		neighborhood[22] = getPixel(image, x,   y,   z+1);
		neighborhood[23] = getPixel(image, x+1, y,   z+1);
		
		neighborhood[24] = getPixel(image, x-1, y+1, z+1);
		neighborhood[25] = getPixel(image, x,   y+1, z+1);
		neighborhood[26] = getPixel(image, x+1, y+1, z+1);
		
		return neighborhood;
	} /* end getNeighborhood */
	
	public byte[] getNeighborhood(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		byte[] neighborhood = new byte[27];
		
		neighborhood[ 0] = getPixel(ra, x-1, y-1, z-1);
		neighborhood[ 1] = getPixel(ra, x  , y-1, z-1);
		neighborhood[ 2] = getPixel(ra, x+1, y-1, z-1);
		
		neighborhood[ 3] = getPixel(ra, x-1, y,   z-1);
		neighborhood[ 4] = getPixel(ra, x,   y,   z-1);
		neighborhood[ 5] = getPixel(ra, x+1, y,   z-1);
		
		neighborhood[ 6] = getPixel(ra, x-1, y+1, z-1);
		neighborhood[ 7] = getPixel(ra, x,   y+1, z-1);
		neighborhood[ 8] = getPixel(ra, x+1, y+1, z-1);
		
		neighborhood[ 9] = getPixel(ra, x-1, y-1, z  );
		neighborhood[10] = getPixel(ra, x,   y-1, z  );
		neighborhood[11] = getPixel(ra, x+1, y-1, z  );
		
		neighborhood[12] = getPixel(ra, x-1, y,   z  );
		neighborhood[13] = getPixel(ra, x,   y,   z  );
		neighborhood[14] = getPixel(ra, x+1, y,   z  );
		
		neighborhood[15] = getPixel(ra, x-1, y+1, z  );
		neighborhood[16] = getPixel(ra, x,   y+1, z  );
		neighborhood[17] = getPixel(ra, x+1, y+1, z  );
		
		neighborhood[18] = getPixel(ra, x-1, y-1, z+1);
		neighborhood[19] = getPixel(ra, x,   y-1, z+1);
		neighborhood[20] = getPixel(ra, x+1, y-1, z+1);
		
		neighborhood[21] = getPixel(ra, x-1, y,   z+1);
		neighborhood[22] = getPixel(ra, x,   y,   z+1);
		neighborhood[23] = getPixel(ra, x+1, y,   z+1);
		
		neighborhood[24] = getPixel(ra, x-1, y+1, z+1);
		neighborhood[25] = getPixel(ra, x,   y+1, z+1);
		neighborhood[26] = getPixel(ra, x+1, y+1, z+1);
		
		return neighborhood;
	} /* end getNeighborhood */
	
	public ArrayList<Integer> getNeighborhoodIndicesForSimpleBorderPointsOnEdge(RandomAccess<UnsignedByteType> ra, int x, int y, int z, ArrayList<int[]> simpleBorderPointsOnEdge)
		{
			ArrayList <Integer> indicesForSimpleBorderPointsOnEdge = new ArrayList<Integer>();
			int index=0;
			for(int zPlus=-1; zPlus<=1; zPlus++) {
				for(int yPlus=-1; yPlus<=1; yPlus++) {
					for(int xPlus=-1; xPlus<=1; xPlus++) {
						if (isIndexInSimpleBorderPointsOnEdge(simpleBorderPointsOnEdge, x+xPlus, y+yPlus, z+zPlus)) {
							indicesForSimpleBorderPointsOnEdge.add(index);
						}
						index++;
					}
				}
			}	
			return indicesForSimpleBorderPointsOnEdge;
	}
	
	public boolean isIndexInSimpleBorderPointsOnEdge(ArrayList<int[]> simpleBorderPointsOnEdge, int x, int y, int z){
		for( int[] index : simpleBorderPointsOnEdge) {
			if(index[0]==x && index[1]==y && index[2]==z) {
				return true;
			}
		}
		return false;
	}
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Get pixel in 3D image (0 border conditions) 
	 * 
	 * @param image 3D image
	 * @param x x- coordinate
	 * @param y y- coordinate
	 * @param z z- coordinate (in image stacks the indexes start at 1)
	 * @return corresponding pixel (0 if out of image)
	 */
	private byte getPixel(ImageStack image, int x, int y, int z)
	{
		if(x >= 0 && x < this.width && y >= 0 && y < this.height && z >= 0 && z < this.depth)
			return ((byte[]) image.getPixels(z + 1))[x + y * this.width];
		else return 0;
	} /* end getPixel */
	
	private byte getPixel(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		if(x >= 0 && x < this.width && y >= 0 && y < this.height && z >= 0 && z < this.depth) {
			ra.setPosition(new int[] {x,y,z});
			if (ra.get().getByte()>0) {
				return 1;
			}
			else {
				return 0;
			}
		}
		else return 0;
	} /* end getPixel */
	
	
	private byte getPixelByte(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		if(x >= 0 && x < this.width && y >= 0 && y < this.height && z >= 0 && z < this.depth) {
			ra.setPosition(new int[] {x,y,z});
			return ra.get().getByte();
		}
		else return 0;
	} /* end getPixel */
	
	private byte getPixelNoCheck(RandomAccess<UnsignedByteType> ra, int x, int y, int z) {
		ra.setPosition(new int[] {x,y,z});
		return ra.get().getByte();
	}
	
	private void setPixel(RandomAccess<UnsignedByteType> ra, int x, int y, int z, byte value)
	{
		if(x >= 0 && x < this.width && y >= 0 && y < this.height && z >= 0 && z < this.depth) {
			ra.setPosition(new int[] {x,y,z});
			ra.get().set(value);
		}
	} /* end setPixel */

	private byte N(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		return getPixel(ra, x, y-1, z);
	} /* end N */
	
	private byte S(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		return getPixel(ra, x, y+1, z);
	} /* end S */
	
	private byte E(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		return getPixel(ra, x+1, y, z);
	} /* end E */
	
	private byte W(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		return getPixel(ra, x-1, y, z);
	} /* end E */
	
	
	private byte U(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		return getPixel(ra, x, y, z+1);
	} /* end U */
	
	private byte B(RandomAccess<UnsignedByteType> ra, int x, int y, int z)
	{
		return getPixel(ra, x, y, z-1);
	} /* end B */
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Fill Euler LUT
	 * 
	 * @param LUT Euler LUT
	 */
	private void fillEulerLUT(int[] LUT) 
	{
		LUT[1]  =  1;
		LUT[3]  = -1;
		LUT[5]  = -1;
		LUT[7]  =  1;
		LUT[9]  = -3;
		LUT[11] = -1;
		LUT[13] = -1;
		LUT[15] =  1;
		LUT[17] = -1;
		LUT[19] =  1;
		LUT[21] =  1;
		LUT[23] = -1;
		LUT[25] =  3;
		LUT[27] =  1;
		LUT[29] =  1;
		LUT[31] = -1;
		LUT[33] = -3;
		LUT[35] = -1;
		LUT[37] =  3;
		LUT[39] =  1;
		LUT[41] =  1;
		LUT[43] = -1;
		LUT[45] =  3;
		LUT[47] =  1;
		LUT[49] = -1;
		LUT[51] =  1;

		LUT[53] =  1;
		LUT[55] = -1;
		LUT[57] =  3;
		LUT[59] =  1;
		LUT[61] =  1;
		LUT[63] = -1;
		LUT[65] = -3;
		LUT[67] =  3;
		LUT[69] = -1;
		LUT[71] =  1;
		LUT[73] =  1;
		LUT[75] =  3;
		LUT[77] = -1;
		LUT[79] =  1;
		LUT[81] = -1;
		LUT[83] =  1;
		LUT[85] =  1;
		LUT[87] = -1;
		LUT[89] =  3;
		LUT[91] =  1;
		LUT[93] =  1;
		LUT[95] = -1;
		LUT[97] =  1;
		LUT[99] =  3;
		LUT[101] =  3;
		LUT[103] =  1;

		LUT[105] =  5;
		LUT[107] =  3;
		LUT[109] =  3;
		LUT[111] =  1;
		LUT[113] = -1;
		LUT[115] =  1;
		LUT[117] =  1;
		LUT[119] = -1;
		LUT[121] =  3;
		LUT[123] =  1;
		LUT[125] =  1;
		LUT[127] = -1;
		LUT[129] = -7;
		LUT[131] = -1;
		LUT[133] = -1;
		LUT[135] =  1;
		LUT[137] = -3;
		LUT[139] = -1;
		LUT[141] = -1;
		LUT[143] =  1;
		LUT[145] = -1;
		LUT[147] =  1;
		LUT[149] =  1;
		LUT[151] = -1;
		LUT[153] =  3;
		LUT[155] =  1;

		LUT[157] =  1;
		LUT[159] = -1;
		LUT[161] = -3;
		LUT[163] = -1;
		LUT[165] =  3;
		LUT[167] =  1;
		LUT[169] =  1;
		LUT[171] = -1;
		LUT[173] =  3;
		LUT[175] =  1;
		LUT[177] = -1;
		LUT[179] =  1;
		LUT[181] =  1;
		LUT[183] = -1;
		LUT[185] =  3;
		LUT[187] =  1;
		LUT[189] =  1;
		LUT[191] = -1;
		LUT[193] = -3;
		LUT[195] =  3;
		LUT[197] = -1;
		LUT[199] =  1;
		LUT[201] =  1;
		LUT[203] =  3;
		LUT[205] = -1;
		LUT[207] =  1;

		LUT[209] = -1;
		LUT[211] =  1;
		LUT[213] =  1;
		LUT[215] = -1;
		LUT[217] =  3;
		LUT[219] =  1;
		LUT[221] =  1;
		LUT[223] = -1;
		LUT[225] =  1;
		LUT[227] =  3;
		LUT[229] =  3;
		LUT[231] =  1;
		LUT[233] =  5;
		LUT[235] =  3;
		LUT[237] =  3;
		LUT[239] =  1;
		LUT[241] = -1;
		LUT[243] =  1;
		LUT[245] =  1;
		LUT[247] = -1;
		LUT[249] =  3;
		LUT[251] =  1;
		LUT[253] =  1;
		LUT[255] = -1;
	}
	
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Fill number of points in octant LUT
	 * 
	 * @param LUT number of points in octant LUT
	 */
	public void fillnumOfPointsLUT(int[] LUT) 
	{
		for(int i=0; i<256; i++)
			LUT[ i ] = Integer.bitCount( i );			
	}

	/**
	 * Check if a point is Euler invariant
	 * 
	 * @param neighbors neighbor pixels of the point
	 * @param LUT Euler LUT
	 * @return true or false if the point is Euler invariant or not
	 */
	boolean isEulerInvariant(byte[] neighbors, int [] LUT)
	{
		// Calculate Euler characteristic for each octant and sum up
		int eulerChar = 0;
		char n;
		// Octant SWU
		n = indexOctantSWU(neighbors);
		eulerChar += LUT[n];
		
		// Octant SEU
		n = indexOctantSEU(neighbors);
		eulerChar += LUT[n];
		
		// Octant NWU
		n = indexOctantNWU(neighbors);
		eulerChar += LUT[n];
		
		// Octant NEU
		n = indexOctantNEU(neighbors);
		eulerChar += LUT[n];
		
		// Octant SWB
		n = indexOctantSWB(neighbors);
		eulerChar += LUT[n];
		
		// Octant SEB
		n = indexOctantSEB(neighbors);
		eulerChar += LUT[n];
		
		// Octant NWB
		n = indexOctantNWB(neighbors);
		eulerChar += LUT[n];
		
		// Octant NEB
		n = indexOctantNEB(neighbors);
		eulerChar += LUT[n];

		return eulerChar == 0;
		}

	public char indexOctantNEB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[2]==1 )
			n |= 128;
		if( neighbors[1]==1 )
			n |=  64;
		if( neighbors[11]==1 )
			n |=  32;
		if( neighbors[10]==1 )
			n |=  16;
		if( neighbors[5]==1 )
			n |=   8;
		if( neighbors[4]==1 )
			n |=   4;
		if( neighbors[14]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantNWB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[0]==1 )
			n |= 128;
		if( neighbors[9]==1 )
			n |=  64;
		if( neighbors[3]==1 )
			n |=  32;
		if( neighbors[12]==1 )
			n |=  16;
		if( neighbors[1]==1 )
			n |=   8;
		if( neighbors[10]==1 )
			n |=   4;
		if( neighbors[4]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSEB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[8]==1 )
			n |= 128;
		if( neighbors[7]==1 )
			n |=  64;
		if( neighbors[17]==1 )
			n |=  32;
		if( neighbors[16]==1 )
			n |=  16;
		if( neighbors[5]==1 )
			n |=   8;
		if( neighbors[4]==1 )
			n |=   4;
		if( neighbors[14]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSWB(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[6]==1 )
			n |= 128;
		if( neighbors[15]==1 )
			n |=  64;
		if( neighbors[7]==1 )
			n |=  32;
		if( neighbors[16]==1 )
			n |=  16;
		if( neighbors[3]==1 )
			n |=   8;
		if( neighbors[12]==1 )
			n |=   4;
		if( neighbors[4]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantNEU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[20]==1 )
			n |= 128;
		if( neighbors[23]==1 )
			n |=  64;
		if( neighbors[19]==1 )
			n |=  32;
		if( neighbors[22]==1 )
			n |=  16;
		if( neighbors[11]==1 )
			n |=   8;
		if( neighbors[14]==1 )
			n |=   4;
		if( neighbors[10]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantNWU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[18]==1 )
			n |= 128;
		if( neighbors[21]==1 )
			n |=  64;
		if( neighbors[9]==1 )
			n |=  32;
		if( neighbors[12]==1 )
			n |=  16;
		if( neighbors[19]==1 )
			n |=   8;
		if( neighbors[22]==1 )
			n |=   4;
		if( neighbors[10]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSEU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[26]==1 )
			n |= 128;
		if( neighbors[23]==1 )
			n |=  64;
		if( neighbors[17]==1 )
			n |=  32;
		if( neighbors[14]==1 )
			n |=  16;
		if( neighbors[25]==1 )
			n |=   8;
		if( neighbors[22]==1 )
			n |=   4;
		if( neighbors[16]==1 )
			n |=   2;
		return n;
	}

	public char indexOctantSWU(byte[] neighbors) {
		char n;
		n = 1;
		if( neighbors[24]==1 )
			n |= 128;
		if( neighbors[25]==1 )
			n |=  64;
		if( neighbors[15]==1 )
			n |=  32;
		if( neighbors[16]==1 )
			n |=  16;
		if( neighbors[21]==1 )
			n |=   8;
		if( neighbors[22]==1 )
			n |=   4;
		if( neighbors[12]==1 )
			n |=   2;
		return n;
	}
	
	/* -----------------------------------------------------------------------*/
	/**
	 * Check if current point is a Simple Point.
	 * This method is named 'N(v)_labeling' in [Lee94].
	 * Outputs the number of connected objects in a neighborhood of a point
	 * after this point would have been removed.
	 * 
	 * @param neighbors neighbor pixels of the point
	 * @return true or false if the point is simple or not
	 */
	private boolean isSimplePoint(byte[] neighbors) 
	{
		// copy neighbors for labeling
		int cube[] = new int[26];
		int i;
		for( i = 0; i < 13; i++ )  // i =  0..12 -> cube[0..12]
			cube[i] = neighbors[i];
		// i != 13 : ignore center pixel when counting (see [Lee94])
		for( i = 14; i < 27; i++ ) // i = 14..26 -> cube[13..25]
			cube[i-1] = neighbors[i];
		// set initial label
		int label = 2;
		// for all points in the neighborhood
		for( i = 0; i < 26; i++ )
		{
			if( cube[i]==1 )     // voxel has not been labeled yet
			{
				// start recursion with any octant that contains the point i
				switch( i )
				{
				case 0:
				case 1:
				case 3:
				case 4:
				case 9:
				case 10:
				case 12:
					octreeLabeling(1, label, cube );
					break;
				case 2:
				case 5:
				case 11:
				case 13:
					octreeLabeling(2, label, cube );
					break;
				case 6:
				case 7:
				case 14:
				case 15:
					octreeLabeling(3, label, cube );
					break;
				case 8:
				case 16:
					octreeLabeling(4, label, cube );
					break;
				case 17:
				case 18:
				case 20:
				case 21:
					octreeLabeling(5, label, cube );
					break;
				case 19:
				case 22:
					octreeLabeling(6, label, cube );
					break;
				case 23:
				case 24:
					octreeLabeling(7, label, cube );
					break;
				case 25:
					octreeLabeling(8, label, cube );
					break;
				}
				label++;
				if( label-2 >= 2 )
				{
					return false;
				}
			}
		}
		//return label-2; in [Lee94] if the number of connected components would be needed
		return true;
	}
	
	
	/* -----------------------------------------------------------------------*/
	/**
	 * This is a recursive method that calculates the number of connected
	 * components in the 3D neighborhood after the center pixel would
	 * have been removed.
	 * 
	 * @param octant
	 * @param label
	 * @param cube
	 */
	private void octreeLabeling(int octant, int label, int[] cube) 
	{
		// check if there are points in the octant with value 1
		  if( octant==1 )
		  {
		  	// set points in this octant to current label
		  	// and recursive labeling of adjacent octants
		    if( cube[0] == 1 )
		      cube[0] = label;
		    if( cube[1] == 1 )
		    {
		      cube[1] = label;        
		      octreeLabeling( 2, label, cube);
		    }
		    if( cube[3] == 1 )
		    {
		      cube[3] = label;        
		      octreeLabeling( 3, label, cube);
		    }
		    if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[9] == 1 )
		    {
		      cube[9] = label;        
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		  }
		  if( octant==2 )
		  {
		    if( cube[1] == 1 )
		    {
		      cube[1] = label;
		      octreeLabeling( 1, label, cube);
		    }
		    if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[2] == 1 )
		      cube[2] = label;        
		    if( cube[5] == 1 )
		    {
		      cube[5] = label;        
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[11] == 1 )
		    {
		      cube[11] = label;        
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==3 )
		  {
		    if( cube[3] == 1 )
		    {
		      cube[3] = label;        
		      octreeLabeling( 1, label, cube);
		    }
		    if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[6] == 1 )
		      cube[6] = label;        
		    if( cube[7] == 1 )
		    {
		      cube[7] = label;        
		      octreeLabeling( 4, label, cube);
		    }
		    if( cube[14] == 1 )
		    {
		      cube[14] = label;        
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==4 )
		  {
		  	if( cube[4] == 1 )
		    {
		      cube[4] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 3, label, cube);
		    }
		  	if( cube[5] == 1 )
		    {
		      cube[5] = label;        
		      octreeLabeling( 2, label, cube);
		    }
		    if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[7] == 1 )
		    {
		      cube[7] = label;        
		      octreeLabeling( 3, label, cube);
		    }
		    if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[8] == 1 )
		      cube[8] = label;        
		    if( cube[16] == 1 )
		    {
		      cube[16] = label;        
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==5 )
		  {
		  	if( cube[9] == 1 )
		    {
		      cube[9] = label;        
		      octreeLabeling( 1, label, cube);
		    }
		    if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[17] == 1 )
		      cube[17] = label;        
		    if( cube[18] == 1 )
		    {
		      cube[18] = label;        
		      octreeLabeling( 6, label, cube);
		    }
		    if( cube[20] == 1 )
		    {
		      cube[20] = label;        
		      octreeLabeling( 7, label, cube);
		    }
		    if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==6 )
		  {
		  	if( cube[10] == 1 )
		    {
		      cube[10] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[11] == 1 )
		    {
		      cube[11] = label;        
		      octreeLabeling( 2, label, cube);
		    }
		    if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[18] == 1 )
		    {
		      cube[18] = label;        
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 7, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[19] == 1 )
		      cube[19] = label;        
		    if( cube[22] == 1 )
		    {
		      cube[22] = label;        
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==7 )
		  {
		  	if( cube[12] == 1 )
		    {
		      cube[12] = label;        
		      octreeLabeling( 1, label, cube);
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 5, label, cube);
		    }
		  	if( cube[14] == 1 )
		    {
		      cube[14] = label;        
		      octreeLabeling( 3, label, cube);
		    }
		    if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[20] == 1 )
		    {
		      cube[20] = label;        
		      octreeLabeling( 5, label, cube);
		    }
		    if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 8, label, cube);
		    }
		    if( cube[23] == 1 )
		      cube[23] = label;        
		    if( cube[24] == 1 )
		    {
		      cube[24] = label;        
		      octreeLabeling( 8, label, cube);
		    }
		  }
		  if( octant==8 )
		  {
		  	if( cube[13] == 1 )
		    {
		      cube[13] = label;        
		      octreeLabeling( 2, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 6, label, cube);
		    }
		  	if( cube[15] == 1 )
		    {
		      cube[15] = label;        
		      octreeLabeling( 3, label, cube);
		      octreeLabeling( 4, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		  	if( cube[16] == 1 )
		    {
		      cube[16] = label;        
		      octreeLabeling( 4, label, cube);
		    }
		  	if( cube[21] == 1 )
		    {
		      cube[21] = label;        
		      octreeLabeling( 5, label, cube);
		      octreeLabeling( 6, label, cube);
		      octreeLabeling( 7, label, cube);
		    }
		  	if( cube[22] == 1 )
		    {
		      cube[22] = label;        
		      octreeLabeling( 6, label, cube);
		    }
		  	if( cube[24] == 1 )
		    {
		      cube[24] = label;        
		      octreeLabeling( 7, label, cube);
		    }
		  	if( cube[25] == 1 )
		      cube[25] = label;        
		  }
		
	}

	/* -----------------------------------------------------------------------*/
	/**
	 * Show plug-in information.
	 * 
	 */
	void showAbout() 
	{
		IJ.showMessage(
						"About Skeletonize3D...",
						"This plug-in filter produces 3D thinning (skeletonization) of binary 3D images.\n");
	} /* end showAbout */
	/* -----------------------------------------------------------------------*/

	/**
	 * @return 	Number of iterations the thinning algorithm completed on the last run
	 *       	If 0 is returned, then the plugin hasn't been run yet
	 */
	public int getThinningIterations() {
		return iterations;
	}


	@Override
	public int setup(String arg, ImagePlus imp) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public void run(ImageProcessor ip) {
		// TODO Auto-generated method stub
		
	}

} /* end skeletonize3D */

