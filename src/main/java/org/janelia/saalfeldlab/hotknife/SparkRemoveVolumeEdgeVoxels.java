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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.hotknife.util.Grid;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.spark_project.guava.collect.Sets;

import ij.ImageJ;
import net.imagej.ops.Ops.Filter.Gauss;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.Shape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.*;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.janelia.saalfeldlab.hotknife.IOHelper;
import org.janelia.saalfeldlab.hotknife.ops.*;

import net.imglib2.type.logic.NativeBoolType;


/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkRemoveVolumeEdgeVoxels {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/predictions.n5")
		private String inputN5Path = null;
		
		@Option(name = "--inputN5DatasetName", required = true, usage = "input N5 path, e.g. /path/to/input/predictions.n5")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/connected_components.n5")
		private String outputN5Path = null;
		
	
		
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
			return outputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}


	}

	
	/**
	 * Find connected components on a block-by-block basis and write out to
	 * temporary n5.
	 *
	 * Takes as input a threshold intensity, above which voxels are used for
	 * calculating connected components. Parallelization is done using a
	 * blockInformationList.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param maskN5PathName
	 * @param thresholdIntensity
	 * @param blockInformationList
	 * @throws IOException
	 */
	
	
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> void removeVolumeEdgeVoxels(
			final JavaSparkContext sc, final String inputN5Path,final String inputN5DatasetName,
			final String outputN5Path, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] dimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);
				
		// Create output dataset
			String outputN5DatasetName = inputN5DatasetName+"_edgeVoxelsRemoved";
			final N5Writer n5Writer = new N5FSWriter(outputN5Path);
			n5Writer.createGroup(outputN5DatasetName);
			n5Writer.createDataset(outputN5DatasetName, dimensions, blockSize,
					org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
			n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [] offset= blockInformation.gridBlock[0];
			final long [] dimension = blockInformation.gridBlock[1];
			
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			IntervalView<UnsignedLongType> input = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
					),offset, dimension);
			
			if(offset[0]==0 || offset[0]+dimension[0]==dimensions[0] ||
				offset[1]==0 || offset[1]+dimension[1]==dimensions[1] ||
				offset[2]==0 || offset[2]+dimension[2]==dimensions[2]) {
			Cursor<UnsignedLongType> inputCursor = input.cursor();

				while(inputCursor.hasNext()) {
					inputCursor.next();
					long x = inputCursor.getLongPosition(0)+offset[0];
					long y = inputCursor.getLongPosition(1)+offset[1];
					long z = inputCursor.getLongPosition(2)+offset[2];
					
					if(x<8 || x>=dimensions[0]-8 || y<8 || y>=dimensions[1]-8 || z<8 || z>=dimensions[2]-8 ) { //then it is neither pm nor ecs
						inputCursor.get().set(0);
					}
				}
			}
			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(input, n5WriterLocal, outputN5DatasetName, blockInformation.gridBlock[2]);

		});

	}


	
	
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkRemoveVolumeEdgeVoxels");
		
		//Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), options.getInputN5DatasetName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		removeVolumeEdgeVoxels(sc, options.getInputN5Path(), options.getInputN5DatasetName(), options.getOutputN5Path(), blockInformationList);
		sc.close();
	}
}

