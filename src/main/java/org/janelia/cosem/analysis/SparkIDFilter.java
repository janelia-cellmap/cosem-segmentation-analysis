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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.Grid;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkIDFilter {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = true, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "Output dataset suffix")
		private String outputN5DatasetSuffix = "_filteredIDs";
		
		@Option(name = "--idsToKeep", required = false, usage = "List of ids to keep")
		private String idsToKeep = "";
		
		@Option(name = "--idsToDelete", required = false, usage = "List of ids to delete ")
		private String idsToDelete = "";
		
		@Option(name = "--keepAdjacentIDs", required = false, usage = "Flag to keep ids adjacent to those selected to keep")
		private boolean keepAdjacentIDs = false;
	
		@Option(name = "--doVolumeFilter", required = false, usage = "Do volume filtering")
		private boolean doVolumeFilter = false;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Minimum volume cutoff (nm^3)")
		private double minimumVolumeCutoff = 20E6;
		
		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = inputN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}
		
		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public String getIDsToKeep() {
			return idsToKeep;
		}
		
		public String getIDsToDelete() {
			return idsToDelete;
		}
		
		public boolean getKeepAdjacentIDs() {
			return keepAdjacentIDs;
		}
		
		public boolean getDoVolumeFilter() {
			return doVolumeFilter;
		}
		
		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}

	}

	public static final <T extends IntegerType<T> & NativeType<T>> Map<Long,Long> getAdjacentIDs(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final Set<Long> idsToKeep,
			final Set<Long> idsToDelete,
			final List<BlockInformation> blockInformationList) throws IOException{
		
			final N5Reader n5Reader = new N5FSReader(n5Path);
			final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
			JavaRDD<Map<Long,Long>> javaRDDmaps = rdd.map(currentBlockInformation -> {
				final long [][] gridBlock = currentBlockInformation.gridBlock;
				final long[] paddedOffset = new long [] {gridBlock[0][0]-1, gridBlock[0][1]-1, gridBlock[0][2]-1};
				final long[] paddedDimension = new long [] {gridBlock[1][0]+2, gridBlock[1][1]+2, gridBlock[1][2]+2};;
				HashMap<Long, Long> adjacentIDtoParentID = new HashMap<Long,Long>();
				final N5Reader n5BlockReader = new N5FSReader(n5Path);
				boolean show=false;
				if(show) new ImageJ();
				final RandomAccessibleInterval<T> source = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<T>)N5Utils.open(n5BlockReader, datasetName)), paddedOffset,paddedDimension);
				RandomAccess<T> sourceRA = source.randomAccess();
				for(long x=1; x<paddedDimension[0]-1; x++) {
					for(long y=1; y<paddedDimension[1]-1; y++) {
						for(long z=1; z<paddedDimension[2]-1; z++) {
							sourceRA.setPosition(new long[] {x,y,z});
							long currentID = sourceRA.get().getIntegerLong();
							if(currentID>0 && !idsToDelete.contains(currentID) && !adjacentIDtoParentID.containsKey(currentID)) {//then isn't explicitly kept or deleted yet
								outerLoop:
								for(long dx=-1; dx<=1; dx+=2 ) {
									for(long dy=-1; dy<=1; dy+=2) {
										for(long dz=-1; dz<=1; dz+=2) {
											sourceRA.setPosition(new long[] {x+dx,y+dy,z+dz});
											long adjacentID = sourceRA.get().getIntegerLong();
											if(idsToKeep.contains(adjacentID)) {
												adjacentIDtoParentID.put(currentID, adjacentID);
												break outerLoop;
											}
										}
									}
								}
							}
						}
					}
				}
				
				return adjacentIDtoParentID;
				
			});
			
			Map<Long,Long> adjacentIDtoParentID = javaRDDmaps.reduce((a,b) -> {a.putAll(b); return a; });

			return adjacentIDtoParentID;
	}
	
	public static final <T extends IntegerType<T> & NativeType<T>>void filterIDs(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			final String suffix,
			final Set<Long> idsToKeep,
			final Set<Long> idsToDelete,
			final Map<Long,Long> adjacentIDtoParentID,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final int n = dimensions.length;
		

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				datasetName + suffix,
				dimensions,
				blockSize,
				attributes.getDataType(),
				new GzipCompression());
		n5Writer.setAttribute(datasetName + suffix, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		
		boolean onlyKeepSubsetOfIDs = idsToKeep.isEmpty() ? false : true;
		
		rdd.foreach(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			final IntervalView<T> source = Views.offsetInterval(
					(RandomAccessibleInterval<T>)N5Utils.open(n5BlockReader, datasetName), gridBlock[0],gridBlock[1]);
			Cursor<T> sourceCursor = source.cursor();
			while(sourceCursor.hasNext()) {
				sourceCursor.next();
				long currentID = sourceCursor.get().getIntegerLong();
				if(currentID>0) {
					if(onlyKeepSubsetOfIDs) {
						if(adjacentIDtoParentID.containsKey(currentID)) sourceCursor.get().setInteger(adjacentIDtoParentID.get(currentID));
						else if(! idsToKeep.contains(currentID)) sourceCursor.get().setZero();
					}
					else { //then keeping everything except those slated to delete
						if(idsToDelete.contains(currentID)) sourceCursor.get().setZero();
					}
				}
			}
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(source, n5BlockWriter, datasetName + suffix, gridBlock[2]);
		});
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

		final SparkConf conf = new SparkConf().setAppName("SparkIDFiltr");

		String inputN5Path = options.getInputN5Path();
		String inputN5DatasetName = options.getInputN5DatasetName();
		String outputN5Path = options.getOutputN5Path();
				
		//Create block information list
		List<BlockInformation> blockInformationList = buildBlockInformationList(options.getInputN5Path(),
			options.getInputN5DatasetName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Set<Long> idsToKeep = new HashSet<Long>();
		if(!options.getIDsToKeep().isEmpty())
			for(String s :  Arrays.asList(options.getIDsToKeep().split(","))) idsToKeep.add(Long.valueOf(s));
		
		Set<Long> idsToDelete = new HashSet<Long>();
		if(!options.getIDsToDelete().isEmpty())
			for(String s :  Arrays.asList(options.getIDsToDelete().split(","))) idsToDelete.add(Long.valueOf(s));
		
		Map<Long,Long> adjacentIDtoParentID = new HashMap<Long,Long>();
		
		String suffix = options.getOutputN5DatasetSuffix(); 
		if(options.getKeepAdjacentIDs()) {
			if(!idsToKeep.isEmpty()) { //if idsToKeep is empty, then we should keep everything except those in ids to remove
				suffix += "_keptAdjacentIDs";
				adjacentIDtoParentID = getAdjacentIDs(sc, inputN5Path, inputN5DatasetName, outputN5Path, idsToKeep, idsToDelete, blockInformationList);
			}
		}
		
		filterIDs(
				sc,
				inputN5Path,
				inputN5DatasetName,
				outputN5Path,
				suffix,
				idsToKeep,
				idsToDelete,
				adjacentIDtoParentID,
				blockInformationList);
		
		/*if(options.getDoVolumeFilter()) {
			SparkVolumeFilterConnectedComponents.volumeFilterConnectedComponents(sc,  inputN5Path, inputN5DatasetName+suffix, outputN5Path, options.getMinimumVolumeCutoff(), blockInformationList);
		}*/
		sc.close();
		
		//Remove temporary files
	}
}
