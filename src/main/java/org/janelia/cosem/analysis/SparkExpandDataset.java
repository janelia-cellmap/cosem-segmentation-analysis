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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
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
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;


/**
 * Expand dataset
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkExpandDataset {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _expandedForMeshes")
		private String outputN5DatasetSuffix = "_expanded";
		
		@Option(name = "--thresholdIntensityCutoff", required = false, usage = "Threshold intensity cutoff above which objects will be expanded")
		private Integer thresholdIntensityCutoff = 0;
		
		@Option(name = "--expansionInNm", required = false, usage = "Expansion (nm)")
		private double expansionInNm = 12;

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
		
		public Integer getThresholdIntensityCutoff() {
			return thresholdIntensityCutoff;
		}
		
		public double getExpansionInNm() {
			return expansionInNm;
		}

	}

	/**
	 * Expand dataset
	 * 
	 * @param sc					Spark context
	 * @param n5Path				Input N5 path
	 * @param inputDatasetName		Skeletonization dataset name
	 * @param n5OutputPath			Output N5 path
	 * @param outputDatasetName		Output N5 dataset name
	 * @param expansionInNm			Expansion in Nm
	 * @param blockInformationList	List of block information
	 * @throws IOException
	 */
	public static final <T extends IntegerType<T> & NativeType<T>> void expandDataset(final JavaSparkContext sc, final String n5Path,
			final String inputDatasetName, final String n5OutputPath, final String outputDatasetName, final int thresholdIntensity, final double expansionInNm,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);
		
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, attributes.getDataType(), new GzipCompression());
		double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
		n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		n5Writer.setAttribute(outputDatasetName, "offset", IOHelper.getOffset(n5Reader,inputDatasetName));

		//n5Writer.setAttribute(outputDatasetName, "offset", n5Reader.getAttribute(inputDatasetName, "offset", int[].class));

		double expansionInVoxels = expansionInNm/pixelResolution[0];
		int expansionInVoxelsCeil = (int) Math.ceil(expansionInVoxels);
		double expansionInVoxelsSquared = expansionInVoxels*expansionInVoxels;
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		

		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];//new long[] {64,64,64};//gridBlock[0];////
			long[] dimension = gridBlock[1];
			long[] paddedOffset = new long[]{offset[0]-expansionInVoxelsCeil, offset[1]-expansionInVoxelsCeil, offset[2]-expansionInVoxelsCeil};
			long[] paddedDimension = new long []{dimension[0]+2*expansionInVoxelsCeil, dimension[1]+2*expansionInVoxelsCeil, dimension[2]+2*expansionInVoxelsCeil};
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			

			RandomAccessibleInterval<T> dataset = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<T>)N5Utils.open(n5BlockReader, inputDatasetName)), paddedOffset, paddedDimension);	
			RandomAccess<T> datasetRA = dataset.randomAccess();
			
			final RandomAccessibleInterval<NativeBoolType> converted = Converters.convert(
					dataset,
					(a, b) -> {
						b.set(a.getIntegerLong()>thresholdIntensity);
					},
					new NativeBoolType());
			
			ArrayImg<FloatType, FloatArray> distanceTransform = ArrayImgs.floats(paddedDimension);
			DistanceTransform.binaryTransform(converted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
			RandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
			
			IntervalView<T> expanded = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<T>)N5Utils.open(n5BlockReader, inputDatasetName)), paddedOffset, paddedDimension);	
			RandomAccess<T> expandedRA = expanded.randomAccess();
			
			for(int x=expansionInVoxelsCeil; x<paddedDimension[0]-expansionInVoxelsCeil; x++) {
				for(int y=expansionInVoxelsCeil; y<paddedDimension[1]-expansionInVoxelsCeil; y++) {
					for(int z=expansionInVoxelsCeil; z<paddedDimension[2]-expansionInVoxelsCeil; z++) {
						int pos[] = new int[] {x,y,z};
						distanceTransformRA.setPosition(pos);
						float distanceSquared = distanceTransformRA.get().get();
						if(distanceSquared<=expansionInVoxelsSquared) {
							expandedRA.setPosition(pos);

							Set<List<Integer>> voxelsToCheck = SparkContactSites.getVoxelsToCheckBasedOnDistance(distanceSquared);
							for(List<Integer> voxelToCheck : voxelsToCheck) {
								int dx = voxelToCheck.get(0);
								int dy = voxelToCheck.get(1);
								int dz = voxelToCheck.get(2);
								datasetRA.setPosition(new long[] {pos[0]+dx,pos[1]+dy,pos[2]+dz});
								T currentObjectID = datasetRA.get();
								if(currentObjectID.getIntegerLong() > 0) {
									expandedRA.get().set(currentObjectID);
									break;
								}							
							}
							
						}
					}
				}
			}
		
			RandomAccessibleInterval<T> output = (RandomAccessibleInterval<T>) Views.offsetInterval(expanded,new long[]{expansionInVoxelsCeil,expansionInVoxelsCeil,expansionInVoxelsCeil}, dimension);
			final N5Writer n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
						
		});

	}
	
	/**
	 * Fill in all voxels within expanded region
	 * 
	 * @param expandedRA 			Output expanded random access
	 * @param objectID				Object ID of skeleton
	 * @param pos					Position of skeleton voxel
	 * @param paddedDimension		Padded dimensions
	 * @param expansionInVoxels		Expansion radius in voxels
	 */
	public static <T extends IntegerType<T>>void fillInExpandedRegion(RandomAccess<T> expandedRA, long value, int [] pos, long [] paddedDimension, int expansionInVoxels) {
		int expansionInVoxelsSquared = expansionInVoxels*expansionInVoxels;
		for(int x=pos[0]-expansionInVoxels; x<=pos[0]+expansionInVoxels; x++) {
			for(int y=pos[1]-expansionInVoxels; y<=pos[1]+expansionInVoxels; y++) {
				for(int z=pos[2]-expansionInVoxels; z<=pos[2]+expansionInVoxels; z++) {
					int dx = x-pos[0];
					int dy = y-pos[1];
					int dz = z-pos[2];
					if((dx*dx+dy*dy+dz*dz)<=expansionInVoxelsSquared) {
						if((x>=0 && y>=0 && z>=0) && (x<paddedDimension[0] && y<paddedDimension[1] && z<paddedDimension[2])) {
							expandedRA.setPosition(new int[] {x,y,z});
							expandedRA.get().setInteger(value);
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

		final SparkConf conf = new SparkConf().setAppName("SparkEpandDataset");

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
			 expandDataset(sc, options.getInputN5Path(), currentOrganelle, options.getOutputN5Path(), options.getInputN5DatasetName()+options.getOutputN5DatasetSuffix(),
					options.getThresholdIntensityCutoff(), options.getExpansionInNm(), blockInformationList);

			sc.close();
		}

	}
}
