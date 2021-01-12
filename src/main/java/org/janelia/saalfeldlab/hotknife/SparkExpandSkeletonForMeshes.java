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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;


/**
 * Expand skeleton for visualization purposes when making meshes
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkExpandSkeletonForMeshes {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _expandedForMeshes")
		private String outputN5DatasetSuffix = "_expandedForMeshes";
		
		@Option(name = "--expansionInVoxels", required = false, usage = "Expansion (voxels)")
		private Integer expansionInVoxels = 3;

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
		
		public String getOutputN5Path() {
			if(outputN5Path == null) {
				return inputN5Path;
			}
			else {
				return outputN5Path;
			}
		}
		
		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}
		

		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}
		
		public Integer getExpansionInVoxels() {
			return expansionInVoxels;
		}

	}

	/**
	 * Expand skeletonization (1-voxel thin data) by a set radius to make meshes cleaner and more visible.
	 * 
	 * @param sc					Spark context
	 * @param n5Path				Input N5 path
	 * @param inputDatasetName		Skeletonization dataset name
	 * @param n5OutputPath			Output N5 path
	 * @param outputDatasetName		Output N5 dataset name
	 * @param expansionInVoxels		Expansion in voxels
	 * @param blockInformationList	List of block information
	 * @throws IOException
	 */
	public static final void expandSkeleton(final JavaSparkContext sc, final String n5Path,
			final String inputDatasetName, final String n5OutputPath, final String outputDatasetName, final int expansionInVoxels,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT64, new GzipCompression());
		n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputDatasetName)));

		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		

		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];//new long[] {64,64,64};//gridBlock[0];////
			long[] dimension = gridBlock[1];
			long[] paddedOffset = new long[]{offset[0]-expansionInVoxels, offset[1]-expansionInVoxels, offset[2]-expansionInVoxels};
			long[] paddedDimension = new long []{dimension[0]+2*expansionInVoxels, dimension[1]+2*expansionInVoxels, dimension[2]+2*expansionInVoxels};
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			

			RandomAccessibleInterval<UnsignedLongType> skeletonDataset = (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, inputDatasetName);	
			final IntervalView<UnsignedLongType> skeletons = Views.offsetInterval(Views.extendZero(skeletonDataset), paddedOffset, paddedDimension);
			RandomAccess<UnsignedLongType> skeletonsRA = skeletons.randomAccess();
			IntervalView<UnsignedLongType> expandedSkeletons = Views.offsetInterval(ArrayImgs.unsignedLongs(paddedDimension),new long[]{0,0,0}, paddedDimension);
			RandomAccess<UnsignedLongType> expandedSkeletonsRA = expandedSkeletons.randomAccess();
			
			for(int x=0; x<paddedDimension[0]; x++) {
				for(int y=0; y<paddedDimension[1]; y++) {
					for(int z=0; z<paddedDimension[2]; z++) {
						int pos[] = new int[] {x,y,z};
						skeletonsRA.setPosition(pos);
						long objectID = skeletonsRA.get().get();
						if(objectID>0) {//then it is part of skeleton
							fillInExpandedRegion(expandedSkeletonsRA, objectID, pos, paddedDimension, expansionInVoxels);
						}
					}
				}
			}
			
			IntervalView<UnsignedLongType> output = Views.offsetInterval(expandedSkeletons,new long[]{expansionInVoxels,expansionInVoxels,expansionInVoxels}, dimension);
			final N5Writer n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
						
		});

	}
	
	/**
	 * Fill in all voxels within expanded region
	 * 
	 * @param expandedSkeletonsRA 	Output expanded skeletons random access
	 * @param objectID				Object ID of skeleton
	 * @param pos					Position of skeleton voxel
	 * @param paddedDimension		Padded dimensions
	 * @param expansionInVoxels		Expansion radius in voxels
	 */
	public static void fillInExpandedRegion(RandomAccess<UnsignedLongType> expandedSkeletonsRA, long objectID, int [] pos, long [] paddedDimension, int expansionInVoxels) {
		int expansionInVoxelsSquared = expansionInVoxels*expansionInVoxels;
		for(int x=pos[0]-expansionInVoxels; x<=pos[0]+expansionInVoxels; x++) {
			for(int y=pos[1]-expansionInVoxels; y<=pos[1]+expansionInVoxels; y++) {
				for(int z=pos[2]-expansionInVoxels; z<=pos[2]+expansionInVoxels; z++) {
					int dx = x-pos[0];
					int dy = y-pos[1];
					int dz = z-pos[2];
						if((dx*dx+dy*dy+dz*dz)<=expansionInVoxelsSquared) {
						if((x>=0 && y>=0 && z>=0) && (x<paddedDimension[0] && y<paddedDimension[1] && z<paddedDimension[2])) {
							expandedSkeletonsRA.setPosition(new int[] {x,y,z});
							expandedSkeletonsRA.get().set(objectID);
						}
					}
				}

			}
		}
	}

	/**
	 * Expand skeleton for more visible meshes
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkEpandSkeletonForMeshes");

		// Get all organelles
		String[] organelles = { "" };
		if (options.getInputN5DatasetName() != null) {
			organelles = options.getInputN5DatasetName().split(",");
		} else {
			File file = new File(options.getInputN5Path());
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		System.out.println(Arrays.toString(organelles));

		for (String currentOrganelle : organelles) {
			// Create block information list
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			 expandSkeleton(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(), options.getInputN5DatasetName()+options.getOutputN5DatasetSuffix(),
					options.getExpansionInVoxels(), blockInformationList);

			sc.close();
		}

	}
}
