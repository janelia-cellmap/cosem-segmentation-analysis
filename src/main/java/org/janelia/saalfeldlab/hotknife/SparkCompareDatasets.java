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
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCompareDatasets {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--inputN5DatasetNames", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetNames = null;
		
		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetNames() {
			return inputN5DatasetNames;
		}

	}

	public static final <T extends NativeType<T>>boolean compareDatasets (
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName1,
			final String datasetName2,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName1);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;
				

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Map<List<Long>,Boolean>> javaRDD = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			IntervalView<T> data1 =  Views.offsetInterval((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, datasetName1)
					,offset, dimension);
				
			IntervalView<T> data2 =  Views.offsetInterval((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, datasetName2)
					,offset, dimension);
			
			Cursor<T> data1Cursor = data1.cursor();
			Cursor<T> data2Cursor = data2.cursor();
			RandomAccess<T> data1RA = data1.randomAccess();
			boolean areEqual = true;
			while(data1Cursor.hasNext() ) {//&& areEqual) {
				data1Cursor.next();
				data2Cursor.next();
				if(!data1Cursor.get().valueEquals(data2Cursor.get())) {
					System.out.println((offset[0]+data1Cursor.getIntPosition(0))+" "+(offset[1]+data1Cursor.getIntPosition(1)) + " "+(offset[2]+data1Cursor.getIntPosition(2)));
					areEqual = false;
				}
			}
			
			Map<List<Long>, Boolean> mapping = new HashMap<List<Long>,Boolean>();
			mapping.put(Arrays.asList(offset[0],offset[1],offset[2]), areEqual);
			return mapping;
		});
		
		Map<List<Long>, Boolean> areEqualMap = javaRDD.reduce((a,b) -> {a.putAll(b); return a; });
		
		boolean areEqual = true;
		for(List<Long> key : areEqualMap.keySet()) {
			if(!areEqualMap.get(key)) {
				System.out.println(key);
				areEqual = false;
			}
		}
		
		return areEqual;
	}

	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		//Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		
		//Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}

	

	public static void logMemory(final String context) {
		final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
		final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
		logMsg(context + ", Total: " + totalMem + " MB, Free: " + freeMem + " MB, Delta: " + (totalMem - freeMem)
				+ " MB");
	}

	public static void logMsg(final String msg) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts + " " + msg);
	}
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkCompareDatasets");


		List<String> directoriesToDelete = new ArrayList<String>();
		String [] datasetNames = options.getInputN5DatasetNames().split(",");
			//Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
				datasetNames[0]);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			boolean areEqual = compareDatasets(
					sc,
					options.getInputN5Path(),
					datasetNames[0],
					datasetNames[1],
					blockInformationList);
			System.out.println("Are they equal? " + areEqual);
			sc.close();
		
		//Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);

	}
}
