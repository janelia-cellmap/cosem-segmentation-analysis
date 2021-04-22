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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class ImageCreationHelper {
    public static long [] getDimensions(int [][][] voxelValues) {
	return new long [] {voxelValues.length, voxelValues[0].length, voxelValues[0][0].length};
    }
    
    public static <T extends IntegerType<T>> Img<T> customImage(int [][][] voxelValues, DataType dataType) {
	long[] dimensions = getDimensions(voxelValues);
	Img<T> output;
	if (dataType == DataType.UINT8) {
	    output = (Img<T>) new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType()).create(dimensions);
	}
	else {
	    output = (Img<T>) new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(dimensions);
	}
	RandomAccess<T> outputRA = output.randomAccess();

	for(int x=0; x<dimensions[0]; x++) {
	    for(int y=0; y<dimensions[1]; y++) {
		for(int z=0; z<dimensions[2]; z++) {
		    outputRA.setPosition(new int [] {x,y,z});
		    outputRA.get().setInteger(voxelValues[x][y][z]);
		}
	    }
	}
	
	return output;
    }
   
    
    public static <T extends IntegerType<T>> Cursor<T> customImageC(int [][][] voxelValues, DataType dataType) {
	return (Cursor<T>) customImage(voxelValues, dataType).cursor();
    }
    
    public static <T extends IntegerType<T>> RandomAccess<T> customImageRA(int [][][] voxelValues, DataType dataType) {
	return (RandomAccess<T>) customImage(voxelValues, dataType).randomAccess();
    }
    
    public static <T> RandomAccessibleInterval<T> customImageRAI(int [][][] voxelValues, DataType dataType) {
	return (RandomAccessibleInterval<T>) customImage(voxelValues, dataType);
    }
    
    public static <T extends IntegerType<T>> Img<T> halfFull(DataType dataType) {
	return halfFull(new long[] {50, 50,50}, dataType);
    }
    
    public static <T extends IntegerType<T>> Img<T> halfFull(long [] dimensions, DataType dataType) {
	int [][][] voxelValues = new int[(int) dimensions[0]][(int) dimensions[1]][(int) dimensions[2]];
	for(int x=0; x<dimensions[0]/2; x++) {
	    for(int y=0; y<dimensions[1]; y++) {
		for(int z=0; z<dimensions[2]; z++) {
		    voxelValues[x][y][z] = 1;
		}
	    }
	}
	
	return customImage(voxelValues, dataType);
    }
    
    public static final <T extends NativeType<T>> boolean compareDatasets (Cursor<T> cursorOne, Cursor<T> cursorTwo){
	boolean areEqual = true;
	while(cursorOne.hasNext() ) {//&& areEqual) {
	    cursorOne.next();
	    cursorTwo.next();
	    if (!cursorOne.get().valueEquals(cursorTwo.get())) {
		areEqual = false;
		break;
	    }
	}
	return areEqual;
    }
    
    public static final <T extends NativeType<T>> boolean compareDatasets (Img<T> im1, Img<T> im2){
   	Cursor<T> im1C = im1.cursor();
   	Cursor<T> im2C = im2.cursor();
	boolean areEqual = true;
   	while(im1C.hasNext() ) {//&& areEqual) {
	    im1C.next();
	    im2C.next();
   	    if (!im1C.get().valueEquals(im2C.get())) {
   		areEqual = false;
   		break;
   	    }
   	}
   	return areEqual;
       }
    
    public static void writeCustomImage(String n5Path, String dataset, int [][][] voxelValues, int[] blockSize, DataType dataType) throws IOException {
	final N5Writer n5Writer = new N5FSWriter(n5Path);
	    RandomAccessibleInterval<UnsignedLongType> rai = customImageRAI(voxelValues, dataType);
	    N5Utils.save(rai, n5Writer, dataset, blockSize, new GzipCompression()); 
    }
}

