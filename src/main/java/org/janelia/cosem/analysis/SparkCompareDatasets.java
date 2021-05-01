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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.Grid;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
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

		@Option(name = "--inputN5Path", required = true, usage = "first input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;
		@Option(name = "--inputN5Path2", required = false, usage = "second input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path2 = null;

		@Option(name = "--inputN5DatasetNames", required = false, usage = "Comma separated list: \"firstDataset,secondDataset\"")
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
		
		public String getInputN5Path2() {
			return inputN5Path2;
		}

		public String getInputN5DatasetNames() {
			return inputN5DatasetNames;
		}

	}

	public static final <T extends NativeType<T>>boolean compareDatasets (
			final JavaSparkContext sc,
			final String n5Path1,
			final String n5Path2,
			final String datasetName1,
			final String datasetName2,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader1 = new N5FSReader(n5Path1);
		final N5Reader n5Reader2 = new N5FSReader(n5Path2);

		DataType dataType1 = n5Reader1.getDatasetAttributes(datasetName1).getDataType();
		DataType dataType2 = n5Reader2.getDatasetAttributes(datasetName1).getDataType();
		if(!dataType1.equals(dataType2)) {
		    System.out.println("Different data types!");
		    return false;
		}
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Map<List<Long>,Boolean>> javaRDD = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path1);
			final N5Reader n5BlockReader2 = new N5FSReader(n5Path2);
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			IntervalView<T> data1 =  Views.offsetInterval((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, datasetName1)
					,offset, dimension);
				
			IntervalView<T> data2 =  Views.offsetInterval((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader2, datasetName2)
					,offset, dimension);
			
			Cursor<T> data1Cursor = data1.cursor();
			Cursor<T> data2Cursor = data2.cursor();
			boolean areEqual = true;
			while(data1Cursor.hasNext() ) {
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

	/**
	 * 
	 * @param inputN5Path  Path to first dataset
	 * @param inputN5Path2 Path to second dataset
	 * @param datasetNames Dataset names
	 * @return
	 * @throws IOException
	 */
	public static boolean setupSparkAndCompare(String inputN5Path1, String inputN5Path2, String dataset1) throws IOException {
	    return setupSparkAndCompare(inputN5Path1, inputN5Path2, dataset1, dataset1); //same dataset names
	}

	public static boolean setupSparkAndCompare(String inputN5Path1, String inputN5Path2, String dataset1, String dataset2) throws IOException {
		final SparkConf conf = new SparkConf().setAppName("SparkCompareDatasets");
			//Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(inputN5Path1, dataset1);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			boolean areEqual = compareDatasets(
					sc,
					inputN5Path1,
					inputN5Path2,
					dataset1,
					dataset2,
					blockInformationList);
			sc.close();

			return areEqual;
	}
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;


		String inputN5Path1 = options.getInputN5Path();
		String inputN5Path2 = options.getInputN5Path2() != null ? options.getInputN5Path2() : inputN5Path1;
		String [] datasetNames = options.getInputN5DatasetNames().split(",");
		boolean areEqual = setupSparkAndCompare(inputN5Path1, inputN5Path2, datasetNames[0], datasetNames[1]);
	
		System.out.println("Are they equal? " + areEqual);
	}
}
