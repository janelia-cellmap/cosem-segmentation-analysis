package org.janelia.cosem.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import ch.qos.logback.core.Context;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class ProcessingHelper {
	
	/**
	 * Count how many faces of a voxel are exposed to the surface (a 0 value)
	 * 
	 * @param ra 	Random access for image
	 * @return		Number of faces on surface
	 */
	public static <T extends IntegerType<T> & NativeType<T>> int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<T> ra, long [] offset, long [] dimensions) {
		List<long[]> voxelsToCheckForSurface = new ArrayList<long[]>(); 
		voxelsToCheckForSurface.add(new long[] {-1, 0, 0});
		voxelsToCheckForSurface.add(new long[] {1, 0, 0});
		voxelsToCheckForSurface.add(new long[] {0, -1, 0});
		voxelsToCheckForSurface.add(new long[] {0, 1, 0});
		voxelsToCheckForSurface.add(new long[] {0, 0, -1});
		voxelsToCheckForSurface.add(new long[] {0, 0, 1});
		
		final long pos[] = {ra.getLongPosition(0), ra.getLongPosition(1), ra.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;

		for(long[] currentVoxel : voxelsToCheckForSurface) {
			final long currentPosition[] = {pos[0]+currentVoxel[0], pos[1]+currentVoxel[1], pos[2]+currentVoxel[2]};
			final long currentGlobalPosition[] = {currentPosition[0]+offset[0],currentPosition[1]+offset[1],currentPosition[2]+offset[2]};
			ra.setPosition(currentPosition);
			if(ra.get().getIntegerLong() == 0 && 
					(currentGlobalPosition[0]>=0 && currentGlobalPosition[1]>=0 && currentGlobalPosition[2]>=0 &&
					currentGlobalPosition[0]<dimensions[0] && currentGlobalPosition[1]<dimensions[1] && currentGlobalPosition[2]<dimensions[2])) {
				surfaceAreaContributionOfVoxelInFaces ++;
			}
		}

		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
	
	/**
	 * Log the memory usage for given {@link Context}
	 * @param context	Name to prepend to message
	 */
	public static void logMemory(final String context) {
		final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
		final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
		logMsg(context + ", Total: " + totalMem + " MB, Free: " + freeMem + " MB, Delta: " + (totalMem - freeMem)
				+ " MB");
	}

	/**
	 * Print out the formatted memory usage message
	 * 
	 * @param msg	Message to log
	 */
	public static void logMsg(final String msg) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts + " " + msg);
	}
	
	/**
	 * Convert global position to global ID
	 * @param position		Global position
	 * @param dimensions	Dimensions of original image
	 * @return				Global ID
	 */
	public static Long convertPositionToGlobalID(long[] position, long[] dimensions) {
		Long ID = dimensions[0] * dimensions[1] * position[2]
				+ dimensions[0] * position[1] + position[0] + 1;
		return ID;
	}
	
	/**
	 * Convert global ID to position
	 * @param globalID		Global ID
	 * @param dimensions	Dimensions of original image
	 * @return				Global ID
	 */
	public static long [] convertGlobalIDtoPosition(long globalID, long[] dimensions) {
		long [] pos = new long[3];
		globalID-=1;
		pos[2] = (long)Math.floor(globalID/(dimensions[0]*dimensions[1]));
		globalID-=pos[2]*dimensions[0]*dimensions[1];
		pos[1] = (long)Math.floor(globalID/dimensions[0]);
		globalID-=pos[1]*dimensions[0];
		pos[0] = globalID;
		return pos;
	}
	
	public static DataType createDatasetUsingTemplateDataset(String templateN5Path, String templateDatasetName, String newN5Path, String newDatasetName) throws IOException {
		return createDatasetUsingTemplateDataset(templateN5Path, templateDatasetName, newN5Path, newDatasetName, null);
	}

	public static DataType createDatasetUsingTemplateDataset(String templateN5Path, String templateDatasetName, String newN5Path, String newDatasetName, DataType dataType) throws IOException {
		final N5Reader n5Reader = new N5FSReader(templateN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(templateDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5Writer n5Writer = new N5FSWriter(newN5Path);
		n5Writer.createDataset(newDatasetName, dimensions, blockSize, dataType == null ? attributes.getDataType() : dataType,
				new GzipCompression());
		double[] pixelResolution = IOHelper.getResolution(n5Reader, templateDatasetName);
		n5Writer.setAttribute(newDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		n5Writer.setAttribute(newDatasetName, "offset", IOHelper.getOffset(n5Reader, templateDatasetName));
		return attributes.getDataType();
	}
	
	public static <T extends NumericType<T>> IntervalView< T > getOffsetIntervalExtendZeroRAI(String n5Path, String dataset, long [] offset, long [] dimension) throws IOException {
		final N5Reader n5Reader = new N5FSReader(n5Path);
		return Views.offsetInterval(
				Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5Reader, dataset)),
				offset, dimension);
	}
	
	public static <T extends NumericType<T>> IntervalView< T > getZerosIntegerImageRAI(long [] dimension, DataType dataType) {
		
		if(dataType == DataType.UINT8) {
		    return (IntervalView<T>) Views.offsetInterval(ArrayImgs.unsignedBytes(dimension),new long[]{0,0,0}, dimension);
		}
		else if(dataType == DataType.UINT16) {
		    return (IntervalView<T>) Views.offsetInterval(ArrayImgs.unsignedShorts(dimension),new long[]{0,0,0}, dimension);

		}
		else if(dataType == DataType.UINT32) {
		    return (IntervalView<T>) Views.offsetInterval(ArrayImgs.unsignedInts(dimension),new long[]{0,0,0}, dimension);

		}
		else if(dataType == DataType.UINT64) {
		    return (IntervalView<T>) Views.offsetInterval(ArrayImgs.unsignedLongs(dimension),new long[]{0,0,0}, dimension);
		}
		return null;
	}
	
	public static <T extends NumericType<T>> RandomAccess < T >  getOffsetIntervalExtendZeroRA(String n5Path, String dataset, long [] offset, long [] dimension) throws IOException {
		IntervalView< T > rai = getOffsetIntervalExtendZeroRAI(n5Path, dataset, offset, dimension);
		return rai.randomAccess();
	}
	
	public static <T extends NumericType<T>> Cursor< T >  getOffsetIntervalExtendZeroC(String n5Path, String dataset, long [] offset, long [] dimension) throws IOException {
		IntervalView< T > rai = getOffsetIntervalExtendZeroRAI(n5Path, dataset, offset, dimension);
		return rai.cursor();
	}
}
