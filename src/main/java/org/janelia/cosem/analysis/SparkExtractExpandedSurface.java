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
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * For a given
 * 
 * TODO: start bins at 1 so that wont run into issues with 0 being mixed in with
 * background
 * 
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkExtractExpandedSurface {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
	private String inputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "Onput N5 dataset")
	private String inputN5DatasetName = null;

	@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
	private String outputN5Path = null;

	@Option(name = "--expansionDistance", required = false, usage = "distance cutoff in nm")
	private int expansionDistance = 50;

	public Options(final String[] args) {
	    final CmdLineParser parser = new CmdLineParser(this);
	    try {
		parser.parseArgument(args);
		if (outputN5Path == null)
		    outputN5Path = inputN5Path;
		parsedSuccessfully = true;
	    } catch (final CmdLineException e) {
		parser.printUsage(System.err);
	    }
	}

	public String getInputN5Path() {
	    return inputN5Path;
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputN5Path() {
	    return outputN5Path;
	}

	public int getExpansionDistance() {
	    return expansionDistance;
	}

    }

   
    /**
     * mask of surface plus some extra distance
     * @param <T>
     * @param sc
     * @param n5Path
     * @param inputDatasetName
     * @param n5OutputPath
     * @param distanceCutoff
     * @param blockInformationList
     * @return 
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void extractExpandedSurface(final JavaSparkContext sc,
	    final String n5Path, final String inputDatasetName, final String n5OutputPath, int expansionDistance,
	    final List<BlockInformation> blockInformationList) throws IOException {

	final String outputDatasetName = inputDatasetName+"_expandedSurfaceBy_"+expansionDistance;

	// General information
	N5FSReader n5Reader = new N5FSReader(n5Path);
	DatasetAttributes attributes = new N5FSReader(n5Path).getDatasetAttributes(inputDatasetName);
	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	double expansionDistanceInVoxels = 1.0*expansionDistance/pixelResolution[0];
	final int padding = (int) (Math.ceil(expansionDistanceInVoxels)+1);
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, inputDatasetName, n5OutputPath, outputDatasetName, attributes.getDataType());
	
	final double expansionDistanceInVoxelsSquared = expansionDistanceInVoxels*expansionDistanceInVoxels;

	// calculate distance from medial surface
	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long[] offset = gridBlock[0];
	    final long[] dimension = gridBlock[1];
	    final long[] paddedOffset = {offset[0]-padding, offset[1]-padding, offset[2]-padding};
	    final long[] paddedDimension = {dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};

	    // Get correctly padded distance transform first
	    RandomAccessibleInterval<T> segmentation = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5Path, inputDatasetName, paddedOffset, paddedDimension);

	    final RandomAccessibleInterval<NativeBoolType> sourceBinarized = Converters.convert(segmentation, (a, b) -> {
		b.set(a.getIntegerLong() ==0);
		}, new NativeBoolType());
	    
	    RandomAccess<T> segmentationRA = segmentation.randomAccess();
	    
	    NativeImg<FloatType, ?>  distanceTransform = ArrayImgs.floats(paddedDimension);
	    DistanceTransform.binaryTransform(sourceBinarized, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);

	    RandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
	    for (long x = padding; x < paddedDimension[0]-padding; x++) {
		for (long y = padding; y < paddedDimension[1]-padding; y++) {
		    for (long z = padding; z < paddedDimension[2]-padding; z++) {
			long[] pos = new long[] { x, y, z };
			segmentationRA.setPosition(pos);
			if(segmentationRA.get().getIntegerLong()>0) {//then is in object
			    distanceTransformRA.setPosition(pos);
			    if(distanceTransformRA.get().get()>expansionDistanceInVoxelsSquared) {//then too far from surface
				segmentationRA.get().setZero();
			    }
			}
		    }
		}
	    }
	    IntervalView<T> output = Views.offsetInterval(segmentation, new long[] {padding,padding,padding}, dimension);

	    // Write out volume averaged sheetness
	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
	});

    }

   

    public static void setupSparkAndExtractExpandedSurface(String inputN5Path, String inputN5DatasetName,
	    String outputN5Path, int expansionDistance)
	    throws IOException {
	
	final SparkConf conf = new SparkConf().setAppName("SparkExtractExpandedSurface");

	for (String organelle : inputN5DatasetName.split(",")) {
	    List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
		    organelle);

	    JavaSparkContext sc = new JavaSparkContext(conf);
	    extractExpandedSurface(sc, inputN5Path, inputN5DatasetName, outputN5Path, expansionDistance, blockInformationList);

	    sc.close();
	}
    }

    /**
     * Take input args and perform the calculation
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

	String inputN5Path = options.getInputN5Path();
	String inputN5DatasetName = options.getInputN5DatasetName();
	String outputN5Path = options.getOutputN5Path();
	int expansionDistance = options.getExpansionDistance();

	setupSparkAndExtractExpandedSurface(inputN5Path, inputN5DatasetName, outputN5Path, expansionDistance);

    }

}
