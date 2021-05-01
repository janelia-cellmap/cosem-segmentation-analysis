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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.decomposition.eig.SymmetricQRAlgorithmDecomposition_DDRM;
import org.janelia.cosem.ops.GradientCenter;
import org.janelia.cosem.ops.SimpleGaussRA;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
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
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
  * 
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkErosionAndDilation {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/data.n5.")
	private String inputN5Path = null;

	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/data.n5")
	private String outputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle. ")
	private String inputN5DatasetName = null;
	
	@Option(name = "--distance", required = false, usage = "Distance to erode dataset (nm); dilation distance is 10% more than this distance")
	private float distance = 500;

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

	public String getOutputN5Path() {
	    return outputN5Path;
	}
	
	public float getDistance() {
	    return distance;
	}
    }

  
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void erode(
	    final JavaSparkContext sc, final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    String outputDatasetName, float distance, final List<BlockInformation> blockInformationList) throws IOException {
	final N5Reader n5Reader = new N5FSReader(n5Path);		
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);

	// Create output
	final String erosionName = inputDatasetName + "_eroded";

	
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, inputDatasetName, n5OutputPath,
		erosionName,attributes.getDataType());
	
	rdd.foreach(blockInformation -> {
	    // Get information for processing blocks
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    double distanceCeil = Math.ceil(distance/pixelResolution[0]);
	    double distanceSquared = distanceCeil*distanceCeil;
	    int padding = (int) distanceCeil+1;
	    long[] paddedOffset = new long[] { offset[0] - padding, offset[1] - padding, offset[2] - padding };
	    long[] paddedDimension = new long[] { dimension[0] + 2 * padding, dimension[1] + 2 * padding,
		    dimension[2] + 2 * padding };
	    // based on https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3726292/

	    // Step 1: create mask
	    RandomAccessibleInterval<T> source = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5Path,
		    inputDatasetName, paddedOffset, paddedDimension);
	    final RandomAccessibleInterval<NativeBoolType> sourceConverted = Converters.convert(
			source,
			(a, b) -> {
				b.set(a.getIntegerLong()<1);
			},
			new NativeBoolType());
	    ArrayImg<FloatType, FloatArray> distanceTransform = ArrayImgs.floats(paddedDimension);
	    DistanceTransform.binaryTransform(sourceConverted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
	    
	    ArrayRandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
	    RandomAccess<T> sourceRA = source.randomAccess();
	    for(long x=0; x<paddedDimension[0]; x++) {
		for(long y=0; y<paddedDimension[1]; y++) {
		    for(long z=0; z<paddedDimension[2];z++) {
			long [] pos = new long [] {x,y,z};
			distanceTransformRA.setPosition(pos);
			if(distanceTransformRA.get().get()<=distanceSquared) {
			    sourceRA.setPosition(pos);
			    sourceRA.get().setZero();
			}
		    }
		}
	    }
	
	    
	    source = Views.offsetInterval(source, new long[] { padding, padding, padding },
		    dimension);

	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(source, n5BlockWriter, erosionName, gridBlock[2]);
	   
	});

    }

    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void dilate(
	    final JavaSparkContext sc, final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    String outputDatasetName, float distance, boolean keepProtrusions, final List<BlockInformation> blockInformationList) throws IOException {
	final N5Reader n5Reader = new N5FSReader(n5Path);		
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);

	// Create output
	final String protrusions = inputDatasetName + "_protrusions";
	final String mainBodies = inputDatasetName + "_mainBodies";

	
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, inputDatasetName, n5OutputPath,
		protrusions,attributes.getDataType());
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, inputDatasetName, n5OutputPath,
		mainBodies,attributes.getDataType());
	
	rdd.foreach(blockInformation -> {
	    // Get information for processing blocks
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    double distanceCeil = Math.ceil(distance/pixelResolution[0]);
	    double distanceSquared = distanceCeil*distanceCeil;
	    int padding = (int) distanceCeil+1;
	    long[] paddedOffset = new long[] { offset[0] - padding, offset[1] - padding, offset[2] - padding };
	    long[] paddedDimension = new long[] { dimension[0] + 2 * padding, dimension[1] + 2 * padding,
		    dimension[2] + 2 * padding };
	    // based on https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3726292/

	    // Step 1: create mask
	    RandomAccessibleInterval<T> source = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5Path,
		    inputDatasetName, paddedOffset, paddedDimension);
	    RandomAccessibleInterval<T> mainBodiesRAI = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5Path,
		    inputDatasetName, paddedOffset, paddedDimension);
	    RandomAccessibleInterval<T> erosion = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5OutputPath,
		    inputDatasetName+"_eroded", paddedOffset, paddedDimension);
	    
	    RandomAccess<T> sourceRA = source.randomAccess();
	    RandomAccess<T> mainBodiesRA = mainBodiesRAI.randomAccess();

	 /*   Set<Long> idsInBlock = new HashSet<Long>();
	    for(long x=0; x<paddedDimension[0]; x++) {
		for(long y=0; y<paddedDimension[1]; y++) {
		    for(long z=0; z<paddedDimension[2];z++) {
			long [] pos = new long [] {x,y,z};
			sourceRA.setPosition(pos);
			Long id = sourceRA.get().getIntegerLong();
			if(id!=0) {
			    idsInBlock.add(id);
			}
			
		    }
		}
	    }
	    */
	    
	   // for (long currentID : idsInBlock) {//loop over all ids to make sure don't expand ids into eachother
	    
        	    final RandomAccessibleInterval<NativeBoolType> erosionConverted = Converters.convert(
        		    erosion,
        			(a, b) -> {
        				b.set(a.getIntegerLong()>0);
        			},
        			new NativeBoolType());
        	    ArrayImg<FloatType, FloatArray> distanceTransform = ArrayImgs.floats(paddedDimension);
        	    DistanceTransform.binaryTransform(erosionConverted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);

        	    ArrayRandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
        	    for(long x=0; x<paddedDimension[0]; x++) {
        		for(long y=0; y<paddedDimension[1]; y++) {
        		    for(long z=0; z<paddedDimension[2];z++) {
        			long [] pos = new long [] {x,y,z};
        			sourceRA.setPosition(pos);
        			if(sourceRA.get().getIntegerLong()>0) {//then it was in original  
        			    distanceTransformRA.setPosition(pos);
        			    if(distanceTransformRA.get().get()<=distanceSquared){
      					sourceRA.get().setZero();
        			    }
        			    else {
        				mainBodiesRA.setPosition(pos);
        				mainBodiesRA.get().setZero();
        			    }
        			   
        			}
        		    }
        		}
        	    }
	   // }
	
	    
	    source = Views.offsetInterval(source, new long[] { padding, padding, padding },
		    dimension);
	    
	    mainBodiesRAI = Views.offsetInterval(mainBodiesRAI, new long[] { padding, padding, padding },
		    dimension);

	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(source, n5BlockWriter, protrusions, gridBlock[2]);
	    N5Utils.saveBlock(mainBodiesRAI, n5BlockWriter, mainBodies, gridBlock[2]);
	   
	});

    }


    private static void setupSparkErosionAndDilation(String inputN5Path, String inputN5DatasetName,
	    String outputN5Path, float distance) throws IOException {
	final SparkConf conf = new SparkConf().setAppName("SparkErosionAndDilation");

	String[] organelles = { "" };
	if (inputN5DatasetName != null) {
	    organelles = inputN5DatasetName.split(",");
	} else {
	    File file = new File(inputN5Path);
	    organelles = file.list(new FilenameFilter() {
		@Override
		public boolean accept(File current, String name) {
		    return new File(current, name).isDirectory();
		}
	    });
	}

	System.out.println(Arrays.toString(organelles));

	String finalOutputN5DatasetName = null;
	for (String currentOrganelle : organelles) {
	    finalOutputN5DatasetName = currentOrganelle;

	    // Create block information list
	    List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
		    currentOrganelle);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    erode(sc, inputN5Path, currentOrganelle, outputN5Path, finalOutputN5DatasetName, distance,
		    blockInformationList);
	    
	    //dilate by extra 10% to ensure that small voxel islands are removed
	    dilate(sc, inputN5Path, currentOrganelle, outputN5Path, finalOutputN5DatasetName, 1.1f*distance, true,
		    blockInformationList);
	    
	   /* erode(sc, outputN5Path, finalOutputN5DatasetName+"_protrusions", outputN5Path, finalOutputN5DatasetName+"_protrusions", 1,
		    blockInformationList);
	    dilate(sc, outputN5Path, finalOutputN5DatasetName+"_protrusions", outputN5Path, finalOutputN5DatasetName+"_protrusions", 1, false,
		    blockInformationList);
	    */
	    sc.close();
	}

    }

    /**
     * Calculate sheetness given input args
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

	String inputN5DatasetName = options.getInputN5DatasetName();
	String inputN5Path = options.getInputN5Path();
	String outputN5Path = options.getOutputN5Path();
	float distance = options.getDistance();
	setupSparkErosionAndDilation(inputN5Path, inputN5DatasetName, outputN5Path, distance);

    }

}
