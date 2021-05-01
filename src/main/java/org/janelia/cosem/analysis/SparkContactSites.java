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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.Bressenham3D;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.cosem.util.SparkDirectoryDelete;
import org.janelia.cosem.util.UnionFindDGA;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Get inter- and intra-contact sites between object types, using a
 * predetermined cutoff distance.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkContactSites {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/segmented/data.n5")
	private String inputN5Path = null;

	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/data.n5")
	private String outputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle")
	private String inputN5DatasetName = null;

	@Option(name = "--inputPairs", required = false, usage = "Pairs for which to calculate contacts: eg 'a_to_b,c_to_d'")
	private String inputPairs = null;

	@Option(name = "--contactDistance", required = false, usage = "Contact site distance (nm)")
	private double contactDistance = 10;

	@Option(name = "--minimumVolumeCutoff", required = false, usage = "Minimum contact site volume cutoff (nm^3)")
	private double minimumVolumeCutoff = 35E3;

	@Option(name = "--doSelfContacts", required = false, usage = "Whether to do self contacts, i.e., an organelle's contact to other organelles of the same class")
	private boolean doSelfContacts = false;

	@Option(name = "--doNaiveMethod", required = false, usage = "Use the naive contact site method (not recommended)")
	private boolean doNaiveMethod = false;

	@Option(name = "--skipGeneratingContactBoundaries", required = false, usage = "Skip generating contact boundaries; useful to save time if contact boundaries already exist")
	private boolean skipGeneratingContactBoundaries = false;

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

	public String getInputPairs() {
	    return inputPairs;
	}

	public String getOutputN5Path() {
	    return outputN5Path;
	}

	public double getContactDistance() {
	    return contactDistance;
	}

	public double getMinimumVolumeCutoff() {
	    return minimumVolumeCutoff;
	}

	public boolean getDoSelfContacts() {
	    return doSelfContacts;
	}

	public boolean getDoNaiveMethod() {
	    return doNaiveMethod;
	}

	public boolean getSkipGeneratingContactBoundaries() {
	    return skipGeneratingContactBoundaries;
	}
    }

    /**
     * Calculate contact boundaries around objects
     *
     * For a given segmented dataset, writes out an n5 file which contains halos
     * around organelles within the contact distance. Each is labeled with the
     * corresponding object ID from the segmentation.
     *
     * @param sc                   Spark context
     * @param inputN5Path          Input N5 path
     * @param organelle            Organelle around which halo is calculated
     * @param outputN5Path         Output N5 path
     * @param contactDistance      Contact distance (in nm)
     * @param doSelfContactSites   Whether or not to do self contacts
     * @param blockInformationList List of block information
     * @throws IOException
     */
    public static final void calculateObjectContactBoundaries(final JavaSparkContext sc, final String inputN5Path,
	    final String organelle, final String outputN5Path, final double contactDistance,
	    final boolean doSelfContactSites, List<BlockInformation> blockInformationList) throws IOException {

	if (doSelfContactSites)
	    calculateObjectContactBoundariesWithSelfContacts(sc, inputN5Path, organelle, outputN5Path, contactDistance,
		    blockInformationList);
	else
	    calculateObjectContactBoundaries(sc, inputN5Path, organelle, outputN5Path, contactDistance,
		    blockInformationList);
    }

    /**
     * Calculate contact boundaries around objects when not doing self-contacts.
     *
     * For a given segmented dataset, writes out an n5 file which contains halos
     * around organelles within the contact distance. Each is labeled with the
     * corresponding object ID from the segmentation.
     *
     * @param sc                   Spark context
     * @param inputN5Path          Input N5 path
     * @param organelle            Organelle around which halo is calculated
     * @param outputN5Path         Output N5 path
     * @param contactDistance      Contact distance (in nm)
     * @param blockInformationList List of block information
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void calculateObjectContactBoundaries(final JavaSparkContext sc, final String inputN5Path,
	    final String organelle, final String outputN5Path, final double contactDistance,
	    List<BlockInformation> blockInformationList) throws IOException {
	// Get attributes of input data set
	
	String outputN5DatasetName = organelle + "_contact_boundary_temp_to_delete";
	DataType dataType = ProcessingHelper.createDatasetUsingTemplateDataset(inputN5Path, organelle, outputN5Path, outputN5DatasetName);
	
	
	double[] pixelResolution = IOHelper.getResolution(new N5FSReader(inputN5Path), organelle);
	// Get contact distance in voxels

	// add 1 extra because are going to include surface voxels of other organelle so
	// need an extra distance of 1 voxel
	final double contactDistanceInVoxels = contactDistance / pixelResolution[0] + 1;
	double contactDistanceInVoxelsSquared = contactDistanceInVoxels * contactDistanceInVoxels;// contactDistanceInVoxelsCeiling*contactDistanceInVoxelsCeiling;
	int contactDistanceInVoxelsCeiling = (int) Math.ceil(contactDistanceInVoxels);

	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(currentBlockInformation -> {
	    // Get information for reading in/writing current block. Need to extend
	    // offset/dimension so that can accomodate objects from different blocks
	    long[][] gridBlock = currentBlockInformation.gridBlock;
	    long[] dimension = gridBlock[1];
	    long[] extendedOffset = gridBlock[0].clone();
	    long[] extendedDimension = gridBlock[1].clone();
	    Arrays.setAll(extendedOffset, i -> extendedOffset[i] - contactDistanceInVoxelsCeiling);
	    Arrays.setAll(extendedDimension, i -> extendedDimension[i] + 2 * contactDistanceInVoxelsCeiling);

	    // Read in source block
	    final RandomAccessibleInterval<T> segmentedOrganelle = ProcessingHelper
		    .getOffsetIntervalExtendZeroRAI(inputN5Path, organelle, extendedOffset, extendedDimension);

	    // Get distance transform
	    final RandomAccessibleInterval<BoolType> objectsBinarized = Converters.convert(segmentedOrganelle,
		    (a, b) -> b.set(a.getIntegerLong() > 0), new BoolType());
	    ArrayImg<FloatType, FloatArray> distanceFromObjects = ArrayImgs.floats(extendedDimension);
	    DistanceTransform.binaryTransform(objectsBinarized, distanceFromObjects, DISTANCE_TYPE.EUCLIDIAN);

	    // Output data
	    IntervalView<T> extendedOutput = ProcessingHelper.getZerosIntegerImageRAI(extendedDimension, dataType);

	    // Access input/output
	    final RandomAccess<T> segmentedOrganelleRandomAccess = segmentedOrganelle.randomAccess();
	    final Cursor<T> extendedOutputCursor = extendedOutput.cursor();
	    final Cursor<FloatType> distanceFromObjectsCursor = distanceFromObjects.cursor();

	    // Loop over image and label voxel within halo according to object ID. Note:
	    // This will mean that each voxel only gets one object assignment
	    while (extendedOutputCursor.hasNext()) {
		final T voxelOutput = extendedOutputCursor.next();
		final float distanceFromObjectsSquared = distanceFromObjectsCursor.next().get();
		long[] position = { extendedOutputCursor.getLongPosition(0), extendedOutputCursor.getLongPosition(1),
			extendedOutputCursor.getLongPosition(2) };
		if ((distanceFromObjectsSquared > 0 && distanceFromObjectsSquared <= contactDistanceInVoxelsSquared)
			|| isSurfaceVoxel(segmentedOrganelleRandomAccess, position)) {// then voxel is within distance
		    findAndSetValueToNearestOrganelleID(voxelOutput, distanceFromObjectsSquared, position,
			    segmentedOrganelleRandomAccess);
		}
	    }

	    RandomAccessibleInterval<T> output = Views.offsetInterval(extendedOutput, new long[] {
		    contactDistanceInVoxelsCeiling, contactDistanceInVoxelsCeiling, contactDistanceInVoxelsCeiling },
		    dimension);
	    final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
	    N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

	});
    }

    /**
     * Calculate contact boundaries around objects when doing self-contacts. In this
     * case, need to check if a given contact boundary for one organelle overlaps
     * with another organelle's contact boundary.
     *
     * For a given segmented dataset, writes out an n5 file which contains halos
     * around organelles within the contact distance. Each is labeled with the
     * corresponding object ID from the segmentation.
     *
     * @param sc                   Spark context
     * @param inputN5Path          Input N5 path
     * @param organelle            Organelle around which halo is calculated
     * @param outputN5Path         Output N5 path
     * @param contactDistance      Contact distance (in nm)
     * @param blockInformationList List of block information
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void calculateObjectContactBoundariesWithSelfContacts(
	    final JavaSparkContext sc, final String inputN5Path, final String organelle, final String outputN5Path,
	    final double contactDistance, List<BlockInformation> blockInformationList) throws IOException {

	// Create output datasets
	String organelleContactBoundaryName = organelle + "_contact_boundary_temp_to_delete";
	String organelleContactBoundaryPairsName = organelle + "_pairs_contact_boundary_temp_to_delete";

	// Get pixel resolution
	double[] pixelResolution = IOHelper.getResolution(new N5FSReader(inputN5Path), organelle);

	DataType dataType = ProcessingHelper.createDatasetUsingTemplateDataset(inputN5Path, organelle, outputN5Path,
		organelleContactBoundaryName);
	ProcessingHelper.createDatasetUsingTemplateDataset(inputN5Path, organelle, outputN5Path,
		organelleContactBoundaryPairsName);

	// Get contact distance in voxels
	final double contactDistanceInVoxels = contactDistance / pixelResolution[0] + 1;// cuz will use surface voxels
	double contactDistanceInVoxelsSquared = contactDistanceInVoxels * contactDistanceInVoxels;// contactDistanceInVoxelsCeiling*contactDistanceInVoxelsCeiling;
	int contactDistanceInVoxelsCeiling = (int) Math.ceil(contactDistanceInVoxels);

	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(currentBlockInformation -> {
	    // Get information for reading in/writing current block. Need to extend
	    // offset/dimension so that can accomodate objects from different blocks
	    long[][] gridBlock = currentBlockInformation.gridBlock;
	    long[] dimension = gridBlock[1];
	    long[] extendedOffset = gridBlock[0].clone();
	    long[] extendedDimension = gridBlock[1].clone();
	    Arrays.setAll(extendedOffset, i -> extendedOffset[i] - contactDistanceInVoxelsCeiling);
	    Arrays.setAll(extendedDimension, i -> extendedDimension[i] + 2 * contactDistanceInVoxelsCeiling);

	    // Read in source block
	    final RandomAccessibleInterval<T> segmentedOrganelle = ProcessingHelper
		    .getOffsetIntervalExtendZeroRAI(inputN5Path, organelle, extendedOffset, extendedDimension);

	    // Get distance transform
	    final RandomAccessibleInterval<BoolType> objectsBinarized = Converters.convert(segmentedOrganelle,
		    (a, b) -> b.set(a.getIntegerLong() > 0), new BoolType());
	    ArrayImg<FloatType, FloatArray> distanceFromObjects = ArrayImgs.floats(extendedDimension);
	    DistanceTransform.binaryTransform(objectsBinarized, distanceFromObjects, DISTANCE_TYPE.EUCLIDIAN);

	    // Access input/output
	    final RandomAccess<T> segmentedOrganelleRandomAccess = segmentedOrganelle.randomAccess();
	    IntervalView<T> extendedOutput = ProcessingHelper.getZerosIntegerImageRAI(extendedDimension, dataType);

	    final IntervalView<T> extendedOutputPair = ProcessingHelper.getZerosIntegerImageRAI(extendedDimension, dataType); // if a voxel is within contactDistance of two
								       // organelles of the same type, that is a contact
								       // site

	    final Cursor<T> extendedOutputCursor = extendedOutput.cursor();
	    final Cursor<T> extendedOutputPairCursor = extendedOutputPair.cursor();
	    final Cursor<FloatType> distanceFromObjectsCursor = distanceFromObjects.cursor();

	    // Loop over image and label voxel within halo according to object ID. Note:
	    // This will mean that each voxel only gets one object assignment
	    while (extendedOutputCursor.hasNext()) {
		final T voxelOutput = extendedOutputCursor.next();
		final T voxelOutputPair = extendedOutputPairCursor.next();
		final float distanceFromObjectsSquared = distanceFromObjectsCursor.next().get();
		long[] position = { extendedOutputCursor.getLongPosition(0), extendedOutputCursor.getLongPosition(1),
			extendedOutputCursor.getLongPosition(2) };
		if ((distanceFromObjectsSquared > 0 && distanceFromObjectsSquared <= contactDistanceInVoxelsSquared)
			|| isSurfaceVoxel(segmentedOrganelleRandomAccess, position)) {// then voxel is within distance
		    long objectID = findAndSetValueToNearestOrganelleID(voxelOutput, distanceFromObjectsSquared,
			    position, segmentedOrganelleRandomAccess);
		    if (objectID > 0) {// Check if another organelle has this voxel as part of its contact site
			findAndSetValueToOrganellePairID(objectID, distanceFromObjectsSquared, voxelOutputPair,
				contactDistanceInVoxelsSquared, position, segmentedOrganelleRandomAccess);
		    }
		}
	    }

	    // Crop and write out data
	    RandomAccessibleInterval<T> output = Views.offsetInterval(extendedOutput, new long[] {
		    contactDistanceInVoxelsCeiling, contactDistanceInVoxelsCeiling, contactDistanceInVoxelsCeiling },
		    dimension);
	    RandomAccessibleInterval<T> outputPair = Views.offsetInterval(extendedOutputPair,
		    new long[] { contactDistanceInVoxelsCeiling, contactDistanceInVoxelsCeiling,
			    contactDistanceInVoxelsCeiling },
		    dimension);

	    final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
	    N5Utils.saveBlock(output, n5WriterLocal, organelleContactBoundaryName, gridBlock[2]);
	    N5Utils.saveBlock(outputPair, n5WriterLocal, organelleContactBoundaryPairsName, gridBlock[2]);

	});
    }

    /**
     * For a given voxel that is within the contact distance, set its id to the
     * nearest organelle.
     * 
     * @param voxelOutput                    Voxel to set
     * @param distanceSquared                Squared distance from object
     * @param position                       Voxel position
     * @param segmentedOrganelleRandomAccess Random access for the segmented dataset
     * @return ID of nearest object
     */
    private static <T extends IntegerType<T> & NativeType<T>> long findAndSetValueToNearestOrganelleID(T voxelOutput, float distanceSquared,
	    long[] position, RandomAccess<T> segmentedOrganelleRandomAccess) {
	Set<List<Integer>> voxelsToCheck = getVoxelsToCheckBasedOnDistance(distanceSquared);
	for (List<Integer> voxelToCheck : voxelsToCheck) {
	    int dx = voxelToCheck.get(0);
	    int dy = voxelToCheck.get(1);
	    int dz = voxelToCheck.get(2);
	    segmentedOrganelleRandomAccess
		    .setPosition(new long[] { position[0] + dx, position[1] + dy, position[2] + dz });
	    long currentObjectID = segmentedOrganelleRandomAccess.get().getIntegerLong();
	    if (currentObjectID > 0) {
		voxelOutput.setInteger(currentObjectID);
		return currentObjectID;
	    }
	}
	return 0;
    }

    /**
     * If a voxel is within the contact distance of an organelle, check if it is
     * also within contact distance of another organelle of the same type. This is
     * necessary for checking for intra-type contacts.
     * 
     * @param objectID                       ID of nearest organelle
     * @param distanceFromObjectSquared      Squared distance from nearest organelle
     * @param voxelOutputPair                Voxel in pair image
     * @param contactDistanceInVoxelsSquared Squared contact distance
     * @param position                       Position
     * @param segmentedOrganelleRandomAccess Random access for segmented dataset
     */
    private static <T extends IntegerType<T> & NativeType<T>> void findAndSetValueToOrganellePairID(long objectID, float distanceFromObjectSquared,
	    T voxelOutputPair, double contactDistanceInVoxelsSquared, long[] position,
	    RandomAccess<T> segmentedOrganelleRandomAccess) {
	// For a given voxel outside an object, find the closest object to it and
	// relabel the voxel with the corresponding object ID
	int distanceFromObject = (int) Math.floor(Math.sqrt(distanceFromObjectSquared));
	int distanceFromOrganellePairSquared = Integer.MAX_VALUE;
	long organellePairID = 0;
	for (int radius = distanceFromObject; radius <= Math.sqrt(contactDistanceInVoxelsSquared); radius++) {// must be
													      // at
													      // least
													      // as far
													      // away as
													      // other
													      // organelle
	    int radiusSquared = radius * radius;
	    int radiusMinus1Squared = (radius - 1) * (radius - 1);
	    for (int x = -radius; x <= radius; x++) {
		for (int y = -radius; y <= radius; y++) {
		    for (int z = -radius; z <= radius; z++) {
			int currentDistanceSquared = x * x + y * y + z * z;
			if (currentDistanceSquared > radiusMinus1Squared && currentDistanceSquared <= radiusSquared
				&& currentDistanceSquared <= contactDistanceInVoxelsSquared) {
			    segmentedOrganelleRandomAccess
				    .setPosition(new long[] { position[0] + x, position[1] + y, position[2] + z });
			    long currentObjectID = segmentedOrganelleRandomAccess.get().getIntegerLong();
			    if (currentObjectID > 0 && currentObjectID != objectID) { // then is within contact distance
										      // of another organelle
				if (currentDistanceSquared < distanceFromOrganellePairSquared) { // ensure it is closest
				    distanceFromOrganellePairSquared = currentDistanceSquared;
				    organellePairID = currentObjectID;
				}

			    }
			}
		    }
		}
	    }
	    if (organellePairID > 0) {
		voxelOutputPair.setInteger(organellePairID);
		return;
	    }
	}
	return;
    }

    /**
     * Find connected components of contact sites for LM data on a block-by-block
     * basis and write out to temporary n5. Since this is for LM data, a voxel is
     * part of a contact site if it is in both objects, ie, there are no in-between
     * voxels that are part of the contact site.
     *
     * 
     * @param sc                   Spark context
     * @param inputN5Path          Input N5 path
     * @param organelle1           First organelle name
     * @param organelle2           Second organelle name
     * @param outputN5Path         Output N5 path
     * @param outputN5DatasetName  Output N5 dataset name
     * @param minimumVolumeCutoff  Minimum volume cutoff (nm^3), above which objects
     *                             will be kept
     * @param blockInformationList List of block information
     * @return List of block information
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> List<BlockInformation> blockwiseConnectedComponentsLM(final JavaSparkContext sc,
	    final String inputN5Path, String organelle1, String organelle2, final String outputN5Path,
	    final String outputN5DatasetName, final double minimumVolumeCutoff,
	    List<BlockInformation> blockInformationList) throws IOException {
	boolean sameOrganelleClassTemp = false;
	if (organelle1.equals(organelle2)) {
	    organelle1 += "_contact_boundary_temp_to_delete";
	    organelle2 += "_pairs_contact_boundary_temp_to_delete";
	    sameOrganelleClassTemp = true;
	} else {
	    organelle1 += "_contact_boundary_temp_to_delete";
	    organelle2 += "_contact_boundary_temp_to_delete";
	}
	final boolean sameOrganelleClass = sameOrganelleClassTemp;
	final String organelle1ContactBoundaryString = organelle1;
	final String organelle2ContactBoundaryString = organelle2;

	// Get attributes of input data set
	final N5Reader n5Reader = new N5FSReader(inputN5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
	final int[] blockSize = attributes.getBlockSize();
	final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
	final long[] outputDimensions = attributes.getDimensions();
	final double[] pixelResolution = IOHelper.getResolution(n5Reader, organelle1);

	// Create output dataset
	DataType dataType = ProcessingHelper.createDatasetUsingTemplateDataset(inputN5Path, organelle1, outputN5Path, outputN5DatasetName);

	// Set up rdd to parallelize over blockInformation list and run RDD, which will
	// return updated block information containing list of components on the edge of
	// the corresponding block
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
	    // Get information for reading in/writing current block
	    long[][] gridBlock = currentBlockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    // Read in source block
	    final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);

	    final RandomAccessibleInterval<T> organelle1Data = Views
		    .offsetInterval(Views.extendZero((RandomAccessibleInterval<T>) N5Utils
			    .open(n5ReaderLocal, organelle1ContactBoundaryString)), offset, dimension);
	    final RandomAccessibleInterval<T> organelle2Data = Views
		    .offsetInterval(Views.extendZero((RandomAccessibleInterval<T>) N5Utils
			    .open(n5ReaderLocal, organelle2ContactBoundaryString)), offset, dimension);

	    RandomAccess<T> organelle1DataRA = organelle1Data.randomAccess();
	    RandomAccess<T> organelle2DataRA = organelle2Data.randomAccess();

	    Set<List<Long>> contactSiteOrganellePairsSet = new HashSet<>();
	    for (int x = 0; x < dimension[0]; x++) {
		for (int y = 0; y < dimension[1]; y++) {
		    for (int z = 0; z < dimension[2]; z++) {
			int[] pos = new int[] { x, y, z };
			organelle1DataRA.setPosition(pos);
			organelle2DataRA.setPosition(pos);

			Long organelle1ID = organelle1DataRA.get().getIntegerLong();
			Long organelle2ID = organelle2DataRA.get().getIntegerLong();

			if (organelle1ID > 0 && organelle2ID > 0) { // then is part of a contact site
			    contactSiteOrganellePairsSet.add(Arrays.asList(organelle1ID, organelle2ID));
			}
		    }
		}
	    }

	    long[] currentDimensions = { 0, 0, 0 };
	    organelle1Data.dimensions(currentDimensions);

	    // Create the output based on the current dimensions
	    final IntervalView<UnsignedLongType> output = ProcessingHelper.getZerosIntegerImageRAI(currentDimensions, DataType.UINT64);

	    final IntervalView<UnsignedByteType> currentPairBinarized = ProcessingHelper.getZerosIntegerImageRAI(outputDimensions, DataType.UINT8);   
	    RandomAccess<UnsignedByteType> currentPairBinarizedRA = currentPairBinarized.randomAccess();
	    currentBlockInformation.edgeComponentIDtoVolumeMap = new HashMap<Long, Long>();
	    currentBlockInformation.edgeComponentIDtoOrganelleIDs = new HashMap<Long, List<Long>>();
	    for (List<Long> organellePairs : contactSiteOrganellePairsSet) {
		for (int x = 0; x < dimension[0]; x++) {
		    for (int y = 0; y < dimension[1]; y++) {
			for (int z = 0; z < dimension[2]; z++) {
			    int[] pos = new int[] { x, y, z };
			    organelle1DataRA.setPosition(pos);
			    organelle2DataRA.setPosition(pos);
			    currentPairBinarizedRA.setPosition(pos);
			    Long organelle1ID = organelle1DataRA.get().getIntegerLong();
			    Long organelle2ID = organelle2DataRA.get().getIntegerLong();
			    if (organelle1ID.equals(organellePairs.get(0))
				    && organelle2ID.equals(organellePairs.get(1))) {
				currentPairBinarizedRA.get().set(1);
			    } else if (sameOrganelleClass && (organelle1ID.equals(organellePairs.get(1))
				    && organelle2ID.equals(organellePairs.get(0)))) {// if it is the same organelle
										     // class, then ids can be swapped
										     // since they still correspond to
										     // the same pair
				currentPairBinarizedRA.get().set(1);
			    } else {
				currentPairBinarizedRA.get().set(0);
			    }
			}
		    }
		}

		// Compute the connected components which returns the components along the block
		// edges, and update the corresponding blockInformation object
		int minimumVolumeCutoffInVoxels = (int) Math
			.ceil(minimumVolumeCutoff / Math.pow(pixelResolution[0], 3));

		currentBlockInformation  = SparkConnectedComponents
			.computeConnectedComponentsForContactingPair(currentBlockInformation, currentPairBinarized, output, outputDimensions, blockSizeL, offset,
				1, minimumVolumeCutoffInVoxels);
		for (Long edgeComponentID : currentBlockInformation.currentContactingPairEdgeComponentIDtoVolumeMap.keySet()) {
		    if (sameOrganelleClass) {
			Long o1 = organellePairs.get(0);
			Long o2 = organellePairs.get(1);
			List<Long> sortedPair = o1 < o2 ? Arrays.asList(o1, o2) : Arrays.asList(o2, o1);
			currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, sortedPair);
		    } else {
			currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID,
				Arrays.asList(organellePairs.get(0), organellePairs.get(1)));
		    }
		}

	    }

	    // Write out output to temporary n5 stack
	    final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
	    N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

	    return currentBlockInformation;
	});

	// Run, collect and return blockInformationList
	blockInformationList = javaRDDsets.collect();

	return blockInformationList;
    }

    /**
     * Find connected components of contact sites for EM data on a block-by-block
     * basis and write out to temporary n5.
     *
     * 
     * @param sc                   Spark context
     * @param inputN5Path          Input N5 path
     * @param organelle1           First organelle name
     * @param organelle2           Second organelle name
     * @param outputN5Path         Output N5 path
     * @param outputN5DatasetName  Output N5 dataset name
     * @param minimumVolumeCutoff  Minimum volume cutoff (nm^3), above which objects
     *                             will be kept
     * @param blockInformationList List of block information
     * @param contactDistance      Contact distance
     * @return List of block information
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> List<BlockInformation> blockwiseConnectedComponentsEM(
	    final JavaSparkContext sc, final String inputN5Path, String organelle1, String organelle2,
	    final String outputN5Path, final String outputN5DatasetName, final double minimumVolumeCutoff,
	    double contactDistance, List<BlockInformation> blockInformationList) throws IOException {

	boolean sameOrganelleClassTemp = false;
	String organelle1ContactBoundaryName, organelle2ContactBoundaryName;
	if (organelle1.equals(organelle2)) { // always look at pairs since know that organelle is within its own halo,
					     // basically want to see if there is another organelle in the same distance
	    organelle1ContactBoundaryName = organelle1 + "_pairs_contact_boundary_temp_to_delete";
	    organelle2ContactBoundaryName = organelle2 + "_pairs_contact_boundary_temp_to_delete";
	    sameOrganelleClassTemp = true;
	} else {
	    organelle1ContactBoundaryName = organelle1 + "_contact_boundary_temp_to_delete";
	    organelle2ContactBoundaryName = organelle2 + "_contact_boundary_temp_to_delete";
	}
	final boolean sameOrganelleClass = sameOrganelleClassTemp;

	// Get attributes of input data set
	final N5Reader n5Reader = new N5FSReader(inputN5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
	final int[] blockSize = attributes.getBlockSize();
	final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
	final long[] outputDimensions = attributes.getDimensions();
	final double[] pixelResolution = IOHelper.getResolution(n5Reader, organelle1);

	// Create output dataset, needed in UINT64 for blockwise
	ProcessingHelper.createDatasetUsingTemplateDataset(inputN5Path, organelle1, outputN5Path, outputN5DatasetName, DataType.UINT64);

	// Set up rdd to parallelize over blockInformation list and run RDD, which will
	// return updated block information containing list of components on the edge of
	// the corresponding block
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

	// Get contact distance in voxels
	final double contactDistanceInVoxels = contactDistance / pixelResolution[0];
	double expandedContactDistanceSquared = (contactDistanceInVoxels + 1) * (contactDistanceInVoxels + 1);// expand
													      // by 1
													      // since
													      // including
													      // surface
													      // voxels
	int contactDistanceInVoxelsCeiling = (int) Math.ceil(contactDistanceInVoxels);

	int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff / Math.pow(pixelResolution[0], 3));

	JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
	    // Get information for reading in/writing current block
	    long[][] gridBlock = currentBlockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];
	    long padding = contactDistanceInVoxelsCeiling + 1;
	    long[] paddedOffset = currentBlockInformation.getPaddedOffset(padding);
	    long[] paddedDimension = currentBlockInformation.getPaddedDimension(padding);

	    // Read in source block

	    RandomAccess<T> organelle1DataRA = ProcessingHelper
		    .getOffsetIntervalExtendZeroRA(inputN5Path, organelle1, paddedOffset, paddedDimension);
	    RandomAccess<T> organelle2DataRA = ProcessingHelper
		    .getOffsetIntervalExtendZeroRA(inputN5Path, organelle2, paddedOffset, paddedDimension);
	    ContactSiteInformation csi = null;
	    { // Necessary? scoping to save memory
		RandomAccess<T> organelle1ContactBoundaryDataRA = ProcessingHelper
			.getOffsetIntervalExtendZeroRA(outputN5Path, organelle1ContactBoundaryName, paddedOffset,
				paddedDimension);
		RandomAccess<T> organelle2ContactBoundaryDataRA = ProcessingHelper
			.getOffsetIntervalExtendZeroRA(outputN5Path, organelle2ContactBoundaryName, paddedOffset,
				paddedDimension);

		csi = getContactSiteInformation(paddedDimension, organelle1DataRA, organelle2DataRA,
			organelle1ContactBoundaryDataRA, organelle2ContactBoundaryDataRA, sameOrganelleClass);
	    }

	    // Create the output based on the current dimensions
	    final IntervalView<UnsignedLongType> output = ProcessingHelper.getZerosIntegerImageRAI(dimension, DataType.UINT64);

	    currentBlockInformation.edgeComponentIDtoVolumeMap = new HashMap<Long, Long>();
	    currentBlockInformation.edgeComponentIDtoOrganelleIDs = new HashMap<Long, List<Long>>();

	    // do all organelle pairs separately
	    ContactSiteVoxelHelper contactSiteVoxelHelper = new ContactSiteVoxelHelper();
	    for (List<Long> currentOrganellePair : csi.allOrganellePairs) {
		long organelle1ID = currentOrganellePair.get(0);
		long organelle2ID = currentOrganellePair.get(1);
		if (sameOrganelleClass && organelle1ID == organelle2ID)
		    break; // then it is an object to itself contact site

		Map<List<Long>, List<Long>> allContactSiteVoxels = getContactSiteVoxelsForOrganellePair(csi,
			currentOrganellePair, organelle1DataRA, organelle2DataRA, expandedContactDistanceSquared,
			padding, dimension, paddedDimension);
		contactSiteVoxelHelper.addContactSiteVoxels(allContactSiteVoxels);
	    }

	    for (Map<List<Long>, List<Long>> currentIndependentContactSiteVoxelInformation : contactSiteVoxelHelper.listOfIndepdenentContactSiteVoxelInformation) {
		doConnectedComponents(currentIndependentContactSiteVoxelInformation, sameOrganelleClass, output,
			minimumVolumeCutoffInVoxels, blockSizeL, offset, dimension, outputDimensions,
			currentBlockInformation);
	    }

	    final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
	    N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

	    return currentBlockInformation;
	});

	// Run, collect and return blockInformationList
	blockInformationList = javaRDDsets.collect();

	return blockInformationList;
    }

    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> List<BlockInformation> naiveConnectedComponents(
	    final JavaSparkContext sc, final String inputN5Path, String organelle1, String organelle2,
	    final String outputN5Path, final String outputN5DatasetName, final double minimumVolumeCutoff,
	    List<BlockInformation> blockInformationList) throws IOException {
	boolean sameOrganelleClassTemp = false;
	if (organelle1.equals(organelle2)) {
	    organelle1 += "_contact_boundary_temp_to_delete";
	    organelle2 += "_pairs_contact_boundary_temp_to_delete";
	    sameOrganelleClassTemp = true;
	} else {
	    organelle1 += "_contact_boundary_temp_to_delete";
	    organelle2 += "_contact_boundary_temp_to_delete";
	}
	final boolean sameOrganelleClass = sameOrganelleClassTemp;
	final String organelle1ContactBoundaryString = organelle1;
	final String organelle2ContactBoundaryString = organelle2;

	// Get attributes of input data set
	final N5Reader n5Reader = new N5FSReader(inputN5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
	final int[] blockSize = attributes.getBlockSize();
	final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
	final long[] outputDimensions = attributes.getDimensions();
	final double[] pixelResolution = IOHelper.getResolution(n5Reader, organelle1);

	// Create output dataset
	ProcessingHelper.createDatasetUsingTemplateDataset(inputN5Path, organelle1, outputN5Path, outputN5DatasetName);

	// Set up rdd to parallelize over blockInformation list and run RDD, which will
	// return updated block information containing list of components on the edge of
	// the corresponding block
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
	    // Get information for reading in/writing current block
	    long[][] gridBlock = currentBlockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    // Read in source block

	    final RandomAccessibleInterval<T> organelle1Data = ProcessingHelper
		    .getOffsetIntervalExtendZeroRAI(inputN5Path, organelle1ContactBoundaryString, offset, dimension);
	    RandomAccess<T> organelle1DataRA = organelle1Data.randomAccess();

	    final RandomAccess<T> organelle2DataRA = ProcessingHelper
		    .getOffsetIntervalExtendZeroRA(inputN5Path, organelle2ContactBoundaryString, offset, dimension);

	    Set<List<Long>> contactSiteOrganellePairsSet = new HashSet<>();
	    for (int x = 0; x < dimension[0]; x++) {
		for (int y = 0; y < dimension[1]; y++) {
		    for (int z = 0; z < dimension[2]; z++) {
			int[] pos = new int[] { x, y, z };
			organelle1DataRA.setPosition(pos);
			organelle2DataRA.setPosition(pos);

			Long organelle1ID = organelle1DataRA.get().getIntegerLong();
			Long organelle2ID = organelle2DataRA.get().getIntegerLong();

			if (organelle1ID > 0 && organelle2ID > 0) { // then is part of a contact site
			    contactSiteOrganellePairsSet.add(Arrays.asList(organelle1ID, organelle2ID));
			}
		    }
		}
	    }

	    long[] currentDimensions = { 0, 0, 0 };
	    organelle1Data.dimensions(currentDimensions);
	    // Create the output based on the current dimensions

	    final IntervalView<UnsignedLongType> output = ProcessingHelper.getZerosIntegerImageRAI(outputDimensions, DataType.UINT64);

	    final Img<UnsignedByteType> currentPairBinarized = new ArrayImgFactory<UnsignedByteType>(
		    new UnsignedByteType()).create(currentDimensions);
	    RandomAccess<UnsignedByteType> currentPairBinarizedRA = currentPairBinarized.randomAccess();
	    currentBlockInformation.edgeComponentIDtoVolumeMap = new HashMap();
	    currentBlockInformation.edgeComponentIDtoOrganelleIDs = new HashMap();
	    for (List<Long> organellePairs : contactSiteOrganellePairsSet) {
		for (int x = 0; x < dimension[0]; x++) {
		    for (int y = 0; y < dimension[1]; y++) {
			for (int z = 0; z < dimension[2]; z++) {
			    int[] pos = new int[] { x, y, z };
			    organelle1DataRA.setPosition(pos);
			    organelle2DataRA.setPosition(pos);
			    currentPairBinarizedRA.setPosition(pos);
			    Long organelle1ID = organelle1DataRA.get().getIntegerLong();
			    Long organelle2ID = organelle2DataRA.get().getIntegerLong();
			    if (organelle1ID.equals(organellePairs.get(0))
				    && organelle2ID.equals(organellePairs.get(1))) {
				currentPairBinarizedRA.get().set(1);
			    } else if (sameOrganelleClass && (organelle1ID.equals(organellePairs.get(1))
				    && organelle2ID.equals(organellePairs.get(0)))) {// if it is the same organelle
										     // class, then ids can be swapped
										     // since they still correspond to
										     // the same pair
				currentPairBinarizedRA.get().set(1);
			    } else {
				currentPairBinarizedRA.get().set(0);
			    }
			}
		    }
		}
		// Compute the connected components which returns the components along the block
		// edges, and update the corresponding blockInformation object
		int minimumVolumeCutoffInVoxels = (int) Math
			.ceil(minimumVolumeCutoff / Math.pow(pixelResolution[0], 3));

		currentBlockInformation = SparkConnectedComponents
			.computeConnectedComponentsForContactingPair(currentBlockInformation,currentPairBinarized, output, outputDimensions, blockSizeL, offset,
				1, minimumVolumeCutoffInVoxels, new RectangleShape(1, false));
		for (Long edgeComponentID : currentBlockInformation.currentContactingPairEdgeComponentIDtoVolumeMap.keySet()) {
		    if (sameOrganelleClass) {
			Long o1 = organellePairs.get(0);
			Long o2 = organellePairs.get(1);
			List<Long> sortedPair = o1 < o2 ? Arrays.asList(o1, o2) : Arrays.asList(o2, o1);
			currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, sortedPair);
		    } else {
			currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID,
				Arrays.asList(organellePairs.get(0), organellePairs.get(1)));
		    }
		}

	    }

	    // Write out output to temporary n5 stack
	    final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
	    N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

	    return currentBlockInformation;
	});

	// Run, collect and return blockInformationList
	blockInformationList = javaRDDsets.collect();

	return blockInformationList;
    }

    static class ContactSiteVoxelHelper {
	public List<Map<List<Long>, List<Long>>> listOfIndepdenentContactSiteVoxelInformation;

	public ContactSiteVoxelHelper() {
	    Map<List<Long>, List<Long>> contactSiteVoxelsToOrganellePair = new HashMap<List<Long>, List<Long>>();
	    listOfIndepdenentContactSiteVoxelInformation = new ArrayList<Map<List<Long>, List<Long>>>(
		    Arrays.asList(contactSiteVoxelsToOrganellePair));
	}

	public void addContactSiteVoxels(Map<List<Long>, List<Long>> newContactSiteVoxelsToOrganellePair) {
	    boolean newVoxelsTouchExistingVoxels = false;
	    for (Map<List<Long>, List<Long>> existingContactSiteVoxelsToOrganellePair : listOfIndepdenentContactSiteVoxelInformation) {
		newVoxelsTouchExistingVoxels = doNewVoxelsTouchExistingVoxels(
			existingContactSiteVoxelsToOrganellePair.keySet(),
			newContactSiteVoxelsToOrganellePair.keySet());
		if (!newVoxelsTouchExistingVoxels) {
		    existingContactSiteVoxelsToOrganellePair.putAll(newContactSiteVoxelsToOrganellePair);
		    break;
		}
	    }

	    if (newVoxelsTouchExistingVoxels) {
		listOfIndepdenentContactSiteVoxelInformation.add(newContactSiteVoxelsToOrganellePair);
	    }
	}

	private static boolean doNewVoxelsTouchExistingVoxels(Set<List<Long>> existingVoxels,
		Set<List<Long>> newVoxels) {
	    for (List<Long> newVoxel : newVoxels) {
		for (long dx = -1; dx <= 1; dx++) {
		    for (long dy = -1; dy <= 1; dy++) {
			for (long dz = -1; dz <= 1; dz++) {
			    List<Long> testVoxel = Arrays.asList(newVoxel.get(0) + dx, newVoxel.get(1) + dy,
				    newVoxel.get(2) + dz);
			    if (existingVoxels.contains(testVoxel)) {
				return true;
			    }
			}
		    }
		}
	    }
	    return false;
	}

    }

    /**
     * Get voxels that make up contact sites for a given pair of organelles. A
     * contact site is taken to be the all voxels between two surface voxels on
     * different organelles, if the surface voxels are within the expanded contact
     * distance.
     * 
     * @param csi                            Contact site information
     * @param currentOrganellePair           The current organelle pair being
     *                                       evaluated
     * @param organelle1DataRA               Organelle 1 data random access
     * @param organelle2DataRA               Organelle 2 data random access
     * @param expandedContactDistanceSquared Expanded contact distance squared
     * @param padding                        Padding
     * @param dimension                      Dimension
     * @param paddedDimension                Padded dimension
     * @return Set of contact site voxel coordinates
     */
    public static <T extends IntegerType<T> & NativeType <T>> Map<List<Long>, List<Long>> getContactSiteVoxelsForOrganellePair(ContactSiteInformation csi,
	    List<Long> currentOrganellePair, RandomAccess<T> organelle1DataRA,
	    RandomAccess<T> organelle2DataRA, double expandedContactDistanceSquared, long padding,
	    long[] dimension, long[] paddedDimension) {
	Map<List<Long>, List<Long>> allContactSiteVoxels = new HashMap<List<Long>, List<Long>>();

	List<long[]> organelle1SurfaceVoxels = csi.organelle1SurfaceVoxelsWithinOrganelle2ContactDistance
		.getOrDefault(currentOrganellePair, new ArrayList<long[]>());
	List<long[]> organelle2SurfaceVoxels = csi.organelle2SurfaceVoxelsWithinOrganelle1ContactDistance
		.getOrDefault(currentOrganellePair, new ArrayList<long[]>());

	for (long[] organelle1SurfaceVoxel : organelle1SurfaceVoxels) {
	    for (long[] organelle2SurfaceVoxel : organelle2SurfaceVoxels) {
		if (Math.pow(organelle2SurfaceVoxel[0] - organelle1SurfaceVoxel[0], 2)
			+ Math.pow(organelle2SurfaceVoxel[1] - organelle1SurfaceVoxel[1], 2)
			+ Math.pow(organelle2SurfaceVoxel[2] - organelle1SurfaceVoxel[2],
				2) <= expandedContactDistanceSquared) { // then could be valid
		    List<long[]> voxelsToCheck = Bressenham3D.getLine(organelle1SurfaceVoxel, organelle2SurfaceVoxel);
		    if (!voxelPathEntersObject(organelle1DataRA, organelle2DataRA, voxelsToCheck)) {
			for (long[] validVoxel : voxelsToCheck) {
			    long[] correctedPosition = new long[] { validVoxel[0] - padding, validVoxel[1] - padding,
				    validVoxel[2] - padding };
			    if (correctedPosition[0] >= 0 && correctedPosition[1] >= 0 && correctedPosition[2] >= 0
				    && correctedPosition[0] < dimension[0] && correctedPosition[1] < dimension[1]
				    && correctedPosition[2] < dimension[2]) {
				allContactSiteVoxels.put(
					Arrays.asList(correctedPosition[0], correctedPosition[1], correctedPosition[2]),
					currentOrganellePair);
			    }
			}
		    }
		}
	    }
	}
	return allContactSiteVoxels;
    }

    /**
     * Calculate the connected components for the contact sites between a given
     * organelle pair
     * 
     * @param organelle1ID                Organelle 1 ID
     * @param organelle2ID                Organelle 2 ID
     * @param sameOrganelleClass          True if same organelle class
     * @param allContactSiteVoxels        Set of contact site voxel coordinates
     * @param output                      Output image
     * @param minimumVolumeCutoffInVoxels Minimum contact site size in voxels
     * @param blockSizeL                  Block size
     * @param offset                      Offset
     * @param dimension                   Dimensions
     * @param outputDimensions            Full dataset dimensions
     * @param currentBlockInformation     The current block information
     */
    public static <T extends IntegerType<T>>void doConnectedComponentsForOrganellePairContactSites(long organelle1ID, long organelle2ID,
	    boolean sameOrganelleClass, Set<List<Long>> allContactSiteVoxels, Img<UnsignedLongType> output,
	    int minimumVolumeCutoffInVoxels, long[] blockSizeL, long[] offset, long[] dimension,
	    long outputDimensions[], BlockInformation currentBlockInformation) {
	Img<UnsignedByteType> currentPairBinarized = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
		.create(dimension);
	RandomAccess<UnsignedByteType> currentPairBinarizedRA = currentPairBinarized.randomAccess();
	for (List<Long> contactSiteVoxel : allContactSiteVoxels) {
	    currentPairBinarizedRA.setPosition(
		    new long[] { contactSiteVoxel.get(0), contactSiteVoxel.get(1), contactSiteVoxel.get(2) });
	    currentPairBinarizedRA.get().set(1);
	}

	// Compute the connected components which returns the components along the block
	// edges, and update the corresponding blockInformation object
	currentBlockInformation = SparkConnectedComponents.computeConnectedComponentsForContactingPair(currentBlockInformation,
		currentPairBinarized, output, outputDimensions, blockSizeL, offset, 1, minimumVolumeCutoffInVoxels,
		new RectangleShape(1, false));
	for (Long edgeComponentID : currentBlockInformation.currentContactingPairEdgeComponentIDtoVolumeMap.keySet()) {
	    if (sameOrganelleClass) {
		List<Long> sortedPair = organelle1ID < organelle2ID ? Arrays.asList(organelle1ID, organelle2ID)
			: Arrays.asList(organelle2ID, organelle1ID);
		currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, sortedPair);
	    } else {
		// TODO: Possible fix if two contact sites cross each other such that one
		// becomes discontinuous, what happens? Can this even happen given how we
		// calculate contact sites? Possibly maybe if the bressenham3d line pops from
		// one organelle pair to another
		currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID,
			Arrays.asList(organelle1ID, organelle2ID));
	    }
	}
    }

    public static void doConnectedComponents(Map<List<Long>, List<Long>> currentIndependentContactSiteVoxelInformation,
	    boolean sameOrganelleClass, IntervalView<UnsignedLongType> output, int minimumVolumeCutoffInVoxels,
	    long[] blockSizeL, long[] offset, long[] dimension, long outputDimensions[],
	    BlockInformation currentBlockInformation) {
	Img<UnsignedByteType> currentPairBinarized = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
		.create(dimension);
	RandomAccess<UnsignedByteType> currentPairBinarizedRA = currentPairBinarized.randomAccess();
	for (List<Long> contactSiteVoxel : currentIndependentContactSiteVoxelInformation.keySet()) {
	    currentPairBinarizedRA.setPosition(
		    new long[] { contactSiteVoxel.get(0), contactSiteVoxel.get(1), contactSiteVoxel.get(2) });
	    currentPairBinarizedRA.get().set(1);
	}

	// Compute the connected components which returns the components along the block
	// edges, and update the corresponding blockInformation object
	currentBlockInformation = SparkConnectedComponents.computeConnectedComponentsForContactingPair(currentBlockInformation,
		currentPairBinarized, output, outputDimensions, blockSizeL, offset, 1, minimumVolumeCutoffInVoxels,
		new RectangleShape(1, false));
	for (Long edgeComponentID : currentBlockInformation.currentContactingPairEdgeComponentIDtoVolumeMap.keySet()) {
	    long[] edgeComponentGlobalPos = ProcessingHelper.convertGlobalIDtoPosition(edgeComponentID,
		    outputDimensions);
	    List<Long> organellePair = currentIndependentContactSiteVoxelInformation
		    .get(Arrays.asList(edgeComponentGlobalPos[0] - offset[0], edgeComponentGlobalPos[1] - offset[1],
			    edgeComponentGlobalPos[2] - offset[2]));
	    long organelle1ID = organellePair.get(0);
	    long organelle2ID = organellePair.get(1);
	    if (sameOrganelleClass) {
		List<Long> sortedPair = organelle1ID < organelle2ID ? Arrays.asList(organelle1ID, organelle2ID)
			: Arrays.asList(organelle2ID, organelle1ID);
		currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID, sortedPair);
	    } else {
		currentBlockInformation.edgeComponentIDtoOrganelleIDs.put(edgeComponentID,
			Arrays.asList(organelle1ID, organelle2ID));
	    }
	}
    }

    /**
     * Store relevant information for contact sites in a single class. This
     * information includes maps of organelle surface voxels within cutoff distance
     * of the pair organelle class, and all pairs of organelles that are within the
     * contact site distance.
     * 
     * @param paddedDimension                 Padded dimension
     * @param organelle1DataRA                Organelle 1 data random access
     * @param organelle2DataRA                Organelle 2 data random access
     * @param organelle1ContactBoundaryDataRA Organelle 1 contact boundary random
     *                                        access
     * @param organelle2ContactBoundaryDataRA Organelle 2 contact boundary random
     *                                        access
     * @param sameOrganelleClass              True if same organelle class
     * @return Instance of {@link ContactSiteInformation}
     */
    public static <T extends IntegerType<T> & NativeType<T>>ContactSiteInformation getContactSiteInformation(long[] paddedDimension,
	    RandomAccess<T> organelle1DataRA, RandomAccess<T> organelle2DataRA,
	    RandomAccess<T> organelle1ContactBoundaryDataRA,
	    RandomAccess<T> organelle2ContactBoundaryDataRA, boolean sameOrganelleClass) {
	Map<List<Long>, List<long[]>> organelle1SurfaceVoxelsWithinOrganelle2ContactDistance = new HashMap<List<Long>, List<long[]>>();
	Map<List<Long>, List<long[]>> organelle2SurfaceVoxelsWithinOrganelle1ContactDistance = new HashMap<List<Long>, List<long[]>>();
	Set<List<Long>> allOrganellePairs = new HashSet<List<Long>>();

	boolean surfaceVoxel;
	for (long x = 0; x < paddedDimension[0]; x++) {
	    for (long y = 0; y < paddedDimension[1]; y++) {
		for (long z = 0; z < paddedDimension[2]; z++) {
		    long[] pos = new long[] { x, y, z };
		    surfaceVoxel = false;
		    organelle1ContactBoundaryDataRA.setPosition(pos);
		    organelle2ContactBoundaryDataRA.setPosition(pos);

		    long organelle1ID = 0, organelle2ID = 0;
		    if (organelle2ContactBoundaryDataRA.get().getIntegerLong() > 0 && isSurfaceVoxel(organelle1DataRA, pos)) {// if
														   // it
														   // is
														   // withincutoff
														   // of
														   // other
														   // class
			surfaceVoxel = true;
			organelle1ID = organelle1DataRA.get().getIntegerLong();
			organelle2ID = organelle2ContactBoundaryDataRA.get().getIntegerLong();
			List<long[]> surfaceVoxels = organelle1SurfaceVoxelsWithinOrganelle2ContactDistance
				.getOrDefault(Arrays.asList(organelle1ID, organelle2ID), new ArrayList<long[]>());
			surfaceVoxels.add(pos);
			organelle1SurfaceVoxelsWithinOrganelle2ContactDistance
				.put(Arrays.asList(organelle1ID, organelle2ID), surfaceVoxels);
		    }
		    if (organelle1ContactBoundaryDataRA.get().getIntegerLong() > 0 && isSurfaceVoxel(organelle2DataRA, pos)) {
			organelle1ID = organelle1ContactBoundaryDataRA.get().getIntegerLong();
			organelle2ID = organelle2DataRA.get().getIntegerLong();
			surfaceVoxel = true;
			List<long[]> surfaceVoxels = organelle2SurfaceVoxelsWithinOrganelle1ContactDistance
				.getOrDefault(Arrays.asList(organelle1ID, organelle2ID), new ArrayList<long[]>());
			surfaceVoxels.add(pos);
			organelle2SurfaceVoxelsWithinOrganelle1ContactDistance
				.put(Arrays.asList(organelle1ID, organelle2ID), surfaceVoxels);
		    }
		    if (surfaceVoxel) {
			allOrganellePairs.add((sameOrganelleClass && organelle2ID < organelle1ID)
				? Arrays.asList(organelle2ID, organelle1ID)
				: Arrays.asList(organelle1ID, organelle2ID));
		    }
		}
	    }
	}

	return new ContactSiteInformation(organelle1SurfaceVoxelsWithinOrganelle2ContactDistance,
		organelle2SurfaceVoxelsWithinOrganelle1ContactDistance, allOrganellePairs);
    }

    /**
     * Class to store information for calculating contact sites including organelle
     * surface voxels from class A within cutoff distance of class B, all organelle
     * pairs that have contact sites and all surface voxel locations as unique IDs
     *
     */
    public static class ContactSiteInformation {
	public Map<List<Long>, List<long[]>> organelle1SurfaceVoxelsWithinOrganelle2ContactDistance;
	public Map<List<Long>, List<long[]>> organelle2SurfaceVoxelsWithinOrganelle1ContactDistance;
	public Set<List<Long>> allOrganellePairs;

	/**
	 * Constructor to build contact site information instance.
	 * 
	 * @param organelle1SurfaceVoxelsWithinOrganelle2ContactDistance Organelle 1
	 *                                                               surface voxels
	 *                                                               within
	 *                                                               organelle 2
	 *                                                               contact
	 *                                                               distance
	 * @param organelle2SurfaceVoxelsWithinOrganelle1ContactDistance Organelle 2
	 *                                                               surface voxels
	 *                                                               within
	 *                                                               organelle 1
	 *                                                               contact
	 *                                                               distance
	 * @param allOrganellePairs                                      All organelle
	 *                                                               pairs that have
	 *                                                               contact sites
	 */
	ContactSiteInformation(Map<List<Long>, List<long[]>> organelle1SurfaceVoxelsWithinOrganelle2ContactDistance,
		Map<List<Long>, List<long[]>> organelle2SurfaceVoxelsWithinOrganelle1ContactDistance,
		Set<List<Long>> allOrganellePairs) {
	    this.organelle1SurfaceVoxelsWithinOrganelle2ContactDistance = organelle1SurfaceVoxelsWithinOrganelle2ContactDistance;
	    this.organelle2SurfaceVoxelsWithinOrganelle1ContactDistance = organelle2SurfaceVoxelsWithinOrganelle1ContactDistance;
	    this.allOrganellePairs = allOrganellePairs;
	}

    }

    /**
     * Union find to determine all necessary objects to merge from blockwise
     * connected components. Can choose diamond/rectangular shape.
     * 
     * Determines which objects need to be fused based on which ones touch at the
     * boundary between blocks. Then performs the corresponding union find so each
     * complete object has a unique id. Parallelizes over block information list.
     * 
     * TODO pull this code and the rest of the code related to this function out
     * into {@link SparkConnectedComponents}
     * 
     * @param sc                            Spark context
     * @param inputN5Path                   Input N5 path
     * @param inputN5DatasetName            Input N5 dataset name
     * @param minimumVolumeCutoff           Minimum volume cutoff, above which
     *                                      objects will be kept
     * @param diamondShape                  Do diamond shape if true, else do
     *                                      rectangular shape
     * @param edgeComponentIDtoOrganelleIDs Map of edge component IDs to organelle
     *                                      IDs, required when doing contact sites
     *                                      to ensure not merging two different
     *                                      contact sites that may be touching
     * @param blockInformationList          Block information list
     * @return Block information list
     * @throws IOException
     */
    public static final List<BlockInformation> unionFindConnectedComponents(final JavaSparkContext sc,
	    final String inputN5Path, final String inputN5DatasetName, double minimumVolumeCutoff,
	    final HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs, boolean diamondShape,
	    List<BlockInformation> blockInformationList) throws IOException {

	// Get attributes of input data set:
	final N5Reader n5Reader = new N5FSReader(inputN5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
	final int[] blockSize = attributes.getBlockSize();
	final double[] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);

	// Set up and run RDD, which will return the set of pairs of object IDs that
	// need to be fused
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<Set<List<Long>>> javaRDDsets = rdd.map(currentBlockInformation -> {
	    // Get information for reading in/writing current block
	    long[][] gridBlock = currentBlockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    // Get source
	    final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
	    @SuppressWarnings("unchecked")
	    final RandomAccessibleInterval<UnsignedLongType> source = (RandomAccessibleInterval<UnsignedLongType>) N5Utils
		    .open(n5ReaderLocal, inputN5DatasetName);
	    long[] sourceDimensions = { 0, 0, 0 };
	    source.dimensions(sourceDimensions);

	    // Get hyperplanes of current block (x-most edge, y-most edge, z-most edge), and
	    // the corresponding hyperplanes for the x+1 block, y+1 block and z+1 block.
	    RandomAccessibleInterval<UnsignedLongType> xPlane1, yPlane1, zPlane1, xPlane2, yPlane2, zPlane2;
	    xPlane1 = yPlane1 = zPlane1 = xPlane2 = yPlane2 = zPlane2 = null;

	    long xOffset = offset[0] + blockSize[0];
	    long yOffset = offset[1] + blockSize[1];
	    long zOffset = offset[2] + blockSize[2];

	    long padding = diamondShape ? 0 : 1; // if using rectangle shape, pad extra of 1 for neighboring block
	    long[] paddedOffset = new long[] { offset[0] - padding, offset[1] - padding, offset[2] - padding };
	    long[] xPlaneDims = new long[] { 1, dimension[1] + 2 * padding, dimension[2] + 2 * padding };
	    long[] yPlaneDims = new long[] { dimension[0] + 2 * padding, 1, dimension[2] + 2 * padding };
	    long[] zPlaneDims = new long[] { dimension[0] + 2 * padding, dimension[1] + 2 * padding, 1 };
	    xPlane1 = Views.offsetInterval(Views.extendZero(source),
		    new long[] { xOffset - 1, paddedOffset[1], paddedOffset[2] }, xPlaneDims);
	    yPlane1 = Views.offsetInterval(Views.extendZero(source),
		    new long[] { paddedOffset[0], yOffset - 1, paddedOffset[2] }, yPlaneDims);
	    zPlane1 = Views.offsetInterval(Views.extendZero(source),
		    new long[] { paddedOffset[0], paddedOffset[1], zOffset - 1 }, zPlaneDims);

	    if (xOffset < sourceDimensions[0])
		xPlane2 = Views.offsetInterval(Views.extendZero(source),
			new long[] { xOffset, paddedOffset[1], paddedOffset[2] }, xPlaneDims);
	    if (yOffset < sourceDimensions[1])
		yPlane2 = Views.offsetInterval(Views.extendZero(source),
			new long[] { paddedOffset[0], yOffset, paddedOffset[2] }, yPlaneDims);
	    if (zOffset < sourceDimensions[2])
		zPlane2 = Views.offsetInterval(Views.extendZero(source),
			new long[] { paddedOffset[0], paddedOffset[1], zOffset }, zPlaneDims);

	    // Calculate the set of object IDs that are touching and need to be merged
	    Set<List<Long>> globalIDtoGlobalIDSet = new HashSet<>();
	    getGlobalIDsToMerge(xPlane1, xPlane2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet, diamondShape);
	    getGlobalIDsToMerge(yPlane1, yPlane2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet, diamondShape);
	    getGlobalIDsToMerge(zPlane1, zPlane2, edgeComponentIDtoOrganelleIDs, globalIDtoGlobalIDSet, diamondShape);
	    // Calculate the set of object IDs that are touching and need to be merged

	    return globalIDtoGlobalIDSet;
	});

	// Collect and combine the sets of objects to merge
	long t0 = System.currentTimeMillis();
	Set<List<Long>> globalIDtoGlobalIDFinalSet = javaRDDsets.reduce((a, b) -> {
	    a.addAll(b);
	    return a;
	});
	long t1 = System.currentTimeMillis();
	System.out.println(globalIDtoGlobalIDFinalSet.size());
	System.out.println("Total unions = " + globalIDtoGlobalIDFinalSet.size());

	// Perform union find to merge all touching objects
	UnionFindDGA unionFind = new UnionFindDGA(globalIDtoGlobalIDFinalSet);
	unionFind.getFinalRoots();

	long t2 = System.currentTimeMillis();

	System.out.println("collect time: " + (t1 - t0));
	System.out.println("union find time: " + (t2 - t1));
	System.out.println("Total edge objects: " + unionFind.globalIDtoRootID.values().stream().distinct().count());

	// Add block-specific relabel map to the corresponding block information object
	Map<Long, Long> rootIDtoVolumeMap = new HashMap<Long, Long>();
	for (BlockInformation currentBlockInformation : blockInformationList) {
	    Map<Long, Long> currentGlobalIDtoRootIDMap = new HashMap<Long, Long>();
	    for (Long currentEdgeComponentID : currentBlockInformation.edgeComponentIDtoVolumeMap.keySet()) {
		Long rootID;
		rootID = unionFind.globalIDtoRootID.getOrDefault(currentEdgeComponentID, currentEdgeComponentID); // Need
														  // this
														  // check
														  // since
														  // not
														  // all
														  // edge
														  // objects
														  // will
														  // be
														  // connected
														  // to
														  // neighboring
														  // blocks
		currentGlobalIDtoRootIDMap.put(currentEdgeComponentID, rootID);
		rootIDtoVolumeMap.put(rootID, rootIDtoVolumeMap.getOrDefault(rootID, 0L)
			+ currentBlockInformation.edgeComponentIDtoVolumeMap.get(currentEdgeComponentID));

	    }
	    currentBlockInformation.edgeComponentIDtoRootIDmap = currentGlobalIDtoRootIDMap;
	}

	int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff / Math.pow(pixelResolution[0], 3));
	for (BlockInformation currentBlockInformation : blockInformationList) {
	    for (Entry<Long, Long> e : currentBlockInformation.edgeComponentIDtoRootIDmap.entrySet()) {
		Long key = e.getKey();
		Long value = e.getValue();
		if(rootIDtoVolumeMap.get(value) <= minimumVolumeCutoffInVoxels) {
		    currentBlockInformation.edgeComponentIDtoRootIDmap.put(key, 0L);
		    currentBlockInformation.allRootIDs.remove(key);
		    currentBlockInformation.allRootIDs.remove(value);
		}else{
		    currentBlockInformation.edgeComponentIDtoRootIDmap.put(key, value);
		    if(! key.equals(value)) { //all we want in the end is the root ids
			currentBlockInformation.allRootIDs.remove(key);
		    }
		}
	    }
	}

	return blockInformationList;
    }

    /**
     * Get the ids on the edges (hypersSlice1 and hyperSlice2) that need to be
     * merged together
     * 
     * @param hyperSlice1                   Edge of block 1
     * @param hyperSlice2                   Edge of block 2
     * @param globalIDtoGlobalIDSet         Set of pairs of IDs to merge
     * @param edgeComponentIDtoOrganelleIDs Map of edge component IDs to organelle
     *                                      IDs, required when doing contact sites
     *                                      to ensure not merging two different
     *                                      contact sites that may be touching
     * @param diamondShape                  If true use diamond shape, else use
     *                                      rectangle shape
     */
    public static final void getGlobalIDsToMerge(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
	    RandomAccessibleInterval<UnsignedLongType> hyperSlice2,
	    final HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs, Set<List<Long>> globalIDtoGlobalIDSet,
	    boolean diamondShape) {
	if (diamondShape)
	    getGlobalIDsToMergeDiamondShape(hyperSlice1, hyperSlice2, edgeComponentIDtoOrganelleIDs,
		    globalIDtoGlobalIDSet);
	else {
	    getGlobalIDsToMergeRectangleShape(hyperSlice1, hyperSlice2, edgeComponentIDtoOrganelleIDs,
		    globalIDtoGlobalIDSet);
	}
    }

    /**
     * Get the ids on the edges (hypersSlice1 and hyperSlice2) that need to be
     * merged together when using diamond shape
     * 
     * @param hyperSlice1                   Edge of block 1
     * @param hyperSlice2                   Edge of block 2
     * @param edgeComponentIDtoOrganelleIDs Map of edge component IDs to organelle
     *                                      IDs, required when doing contact sites
     *                                      to ensure not merging two different
     *                                      contact sites that may be touching
     * @param globalIDtoGlobalIDSet         Set of pairs of IDs to merge
     */
    public static final void getGlobalIDsToMergeDiamondShape(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
	    RandomAccessibleInterval<UnsignedLongType> hyperSlice2,
	    final HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs, Set<List<Long>> globalIDtoGlobalIDSet) {
	// The global IDS that need to be merged are those that are touching along the
	// hyperplane borders between adjacent blocks
	if (hyperSlice1 != null && hyperSlice2 != null) {
	    Cursor<UnsignedLongType> hs1Cursor = Views.flatIterable(hyperSlice1).cursor();
	    Cursor<UnsignedLongType> hs2Cursor = Views.flatIterable(hyperSlice2).cursor();
	    while (hs1Cursor.hasNext()) {
		long hs1Value = hs1Cursor.next().getLong();
		long hs2Value = hs2Cursor.next().getLong();

		List<Long> organelleIDs1 = edgeComponentIDtoOrganelleIDs.get(hs1Value);
		List<Long> organelleIDs2 = edgeComponentIDtoOrganelleIDs.get(hs2Value);
		if (hs1Value > 0 && hs2Value > 0 && organelleIDs1.equals(organelleIDs2)) {
		    globalIDtoGlobalIDSet.add(Arrays.asList(hs1Value, hs2Value));// hs1->hs2 pair should always be
										 // distinct since hs1 is unique to
										 // first block
		}
	    }

	}
    }

    /**
     * Get the ids on the edges (hypersSlice1 and hyperSlice2) that need to be
     * merged together when using rectangle shape
     * 
     * @param hyperSlice1                   Edge of block 1
     * @param hyperSlice2                   Edge of block 2
     * @param edgeComponentIDtoOrganelleIDs Map of edge component IDs to organelle
     *                                      IDs, required when doing contact sites
     *                                      to ensure not merging two different
     *                                      contact sites that may be touching
     * @param globalIDtoGlobalIDSet         Set of pairs of IDs to merge
     */
    public static final void getGlobalIDsToMergeRectangleShape(RandomAccessibleInterval<UnsignedLongType> hyperSlice1,
	    RandomAccessibleInterval<UnsignedLongType> hyperSlice2,
	    final HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs, Set<List<Long>> globalIDtoGlobalIDSet) {
	// The global IDS that need to be merged are those that are touching along the
	// hyperplane borders between adjacent blocks
	if (hyperSlice1 != null && hyperSlice2 != null) {
	    RandomAccess<UnsignedLongType> hs1RA = hyperSlice1.randomAccess();
	    RandomAccess<UnsignedLongType> hs2RA = hyperSlice2.randomAccess();

	    long[] dimensions = new long[] { hyperSlice1.dimension(0), hyperSlice1.dimension(1),
		    hyperSlice1.dimension(2) };

	    List<Long> xPositions = new ArrayList<Long>();
	    List<Long> yPositions = new ArrayList<Long>();
	    List<Long> zPositions = new ArrayList<Long>();

	    List<Long> deltaX = new ArrayList<Long>();
	    List<Long> deltaY = new ArrayList<Long>();
	    List<Long> deltaZ = new ArrayList<Long>();

	    // initialize before we know which dimension the plane is along
	    xPositions = Arrays.asList(1L, dimensions[0] - 2);
	    yPositions = Arrays.asList(1L, dimensions[1] - 2);
	    zPositions = Arrays.asList(1L, dimensions[2] - 2);

	    deltaX = Arrays.asList(-1L, 0L, 1L);
	    deltaY = Arrays.asList(-1L, 0L, 1L);
	    deltaZ = Arrays.asList(-1L, 0L, 1L);

	    // determine plane we are working on
	    if (dimensions[0] == 1) {
		deltaX = Arrays.asList(0L);
		xPositions = Arrays.asList(0L, 0L);
	    } else if (dimensions[1] == 1) {
		deltaY = Arrays.asList(0L);
		yPositions = Arrays.asList(0L, 0L);
	    } else {
		deltaZ = Arrays.asList(0L);
		zPositions = Arrays.asList(0L, 0L);
	    }

	    long hs1Value, hs2Value;
	    for (long x = xPositions.get(0); x <= xPositions.get(1); x++) {
		for (long y = yPositions.get(0); y <= yPositions.get(1); y++) {
		    for (long z = zPositions.get(0); z <= zPositions.get(1); z++) {
			long[] pos = new long[] { x, y, z };
			hs1RA.setPosition(pos);
			hs1Value = hs1RA.get().get();
			if (hs1Value > 0) {
			    for (long dx : deltaX) {
				for (long dy : deltaY) {
				    for (long dz : deltaZ) {
					long[] newPos = new long[] { pos[0] + dx, pos[1] + dy, pos[2] + dz };
					hs2RA.setPosition(newPos);
					hs2Value = hs2RA.get().get();

					if (hs2Value > 0) {
					    List<Long> organelleIDs1 = edgeComponentIDtoOrganelleIDs.get(hs1Value);
					    List<Long> organelleIDs2 = edgeComponentIDtoOrganelleIDs.get(hs2Value);
					    if (organelleIDs1.equals(organelleIDs2))
						globalIDtoGlobalIDSet.add(Arrays.asList(hs1Value, hs2Value));// hs1->hs2
													     // pair
													     // should
													     // always
													     // be
					    // distinct since hs1 is unique to
					    // first block
					}
				    }
				}
			    }
			}
		    }
		}
	    }
	}
    }

    /**
     * Get relative coordinates of all voxels at a given distance
     * 
     * @param distanceSquared The desired distance, squared
     * @return Set of relative positions (as {@link List}) of all voxels at distance
     */
    public static final Set<List<Integer>> getVoxelsToCheckBasedOnDistance(float distanceSquared) {
	Set<List<Integer>> voxelsToCheck = new HashSet<>();
	double distance = Math.sqrt(distanceSquared);
	int[] posNeg = new int[] { 1, -1 };
	for (int i = 0; i <= distance; i++) {
	    for (int j = 0; j <= distance; j++) {
		for (int k = 0; k <= distance; k++) {
		    if (i * i + j * j + k * k == distanceSquared) {
			for (int ip : posNeg) {
			    for (int jp : posNeg) {
				for (int kp : posNeg) {
				    voxelsToCheck.add(Arrays.asList(i * ip, j * jp, k * kp));
				}
			    }
			}
		    }
		}
	    }
	}
	return voxelsToCheck;
    }

    /**
     * Check if a path (set of voxels) enters object (excluding the first and last).
     * 
     * @param organelle1RA  Organelle 1 segmentation data random access
     * @param organelle2RA  Organelle 2 segmentation data random access
     * @param voxelsToCheck Path of voxels to check
     * @return True if passes through object
     */
    public static <T extends IntegerType<T> & NativeType<T>> boolean voxelPathEntersObject(RandomAccess<T> organelle1RA,
	    RandomAccess<T> organelle2RA, List<long[]> voxelsToCheck) {

	for (int i = 1; i < voxelsToCheck.size() - 1; i++) {// don't check start and end since those are defined already
							    // to be surface voxels.
	    long[] currentVoxel = voxelsToCheck.get(i);
	    organelle1RA.setPosition(currentVoxel);
	    if (organelle1RA.get().getIntegerLong() > 0)
		return true;
	    else {
		organelle2RA.setPosition(currentVoxel);
		if (organelle2RA.get().getIntegerLong() > 0)
		    return true;
	    }
	}
	return false;
    }

    /**
     * Perform the contact site analysis after contact boundaries have been
     * calculated.
     * 
     * @param conf                Spark conf
     * @param organelles          List of organelles to process contact sites
     *                            between
     * @param doSelfContacts      True if want to include organelleA-organelleA
     *                            contacts
     * @param minimumVolumeCutoff Minimum volume above which objects will be kept
     * @param contactDistance     Maximum distance surface adjacent organelle voxels
     *                            can be for intervening region to be considered
     *                            contact site. TODO maybe redo this so that contact
     *                            distance is distance between surfaces themselves
     * @param doLM                Do LM (rather than EM)
     * @param inputN5Path         Input N5 path
     * @param outputN5Path        Output N5 path
     * @throws IOException
     */
    public static final void calculateContactSites(final SparkConf conf, final String[] organelles,
	    final boolean doSelfContacts, final double minimumVolumeCutoff, final double contactDistance,
	    final boolean doLM, final String inputN5Path, final String outputN5Path, boolean doNaiveMethod)
	    throws IOException {
	List<String> directoriesToDelete = new ArrayList<String>();
	for (int i = 0; i < organelles.length; i++) {
	    final String organelle1 = organelles[i];
	    for (int j = doSelfContacts ? i : i + 1; j < organelles.length; j++) {
		String organelle2 = organelles[j];
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
			organelle1);
		JavaSparkContext sc = new JavaSparkContext(conf);

		final String organelleContactString = organelle1 + "_to_" + organelle2;
		final String tempOutputN5ConnectedComponents = organelleContactString + "_cc_blockwise_temp_to_delete";
		final String finalOutputN5DatasetName = organelleContactString + "_cc";

		if (contactDistance == 0 && doLM) {
		    blockInformationList = blockwiseConnectedComponentsLM(sc, inputN5Path, organelle1, organelle2,
			    outputN5Path, tempOutputN5ConnectedComponents, minimumVolumeCutoff, blockInformationList);
		} else {
		    if (doNaiveMethod) {
			blockInformationList = naiveConnectedComponents(sc, inputN5Path, organelle1, organelle2,
				outputN5Path, tempOutputN5ConnectedComponents, minimumVolumeCutoff,
				blockInformationList);
		    } else {
			blockInformationList = blockwiseConnectedComponentsEM(sc, inputN5Path, organelle1, organelle2,
				outputN5Path, tempOutputN5ConnectedComponents, minimumVolumeCutoff, contactDistance,
				blockInformationList);
		    }
		}

		HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs = new HashMap<Long, List<Long>>();
		for (BlockInformation currentBlockInformation : blockInformationList) {
		    edgeComponentIDtoOrganelleIDs.putAll(currentBlockInformation.edgeComponentIDtoOrganelleIDs);
		}
		boolean diamondShape = false; // using rectangle
		blockInformationList = unionFindConnectedComponents(sc, outputN5Path, tempOutputN5ConnectedComponents,
			minimumVolumeCutoff, edgeComponentIDtoOrganelleIDs, diamondShape, blockInformationList);

		SparkConnectedComponents.mergeConnectedComponents(sc, outputN5Path, tempOutputN5ConnectedComponents,
			finalOutputN5DatasetName, blockInformationList);
		directoriesToDelete.add(outputN5Path + "/" + tempOutputN5ConnectedComponents);
		sc.close();
	    }
	}

	SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
    }

    /**
     * Check if a voxel is on the surface of an object
     * 
     * @param sourceRandomAccess Random access to check
     * @param position           Voxel postion
     * @return True if it is a surface voxel
     */
    public static <T extends IntegerType<T> & NativeType<T>> boolean isSurfaceVoxel(final RandomAccess<T> sourceRandomAccess, long[] position) {
	sourceRandomAccess.setPosition(position);
	long referenceVoxelValue = sourceRandomAccess.get().getIntegerLong();
	if (referenceVoxelValue == 0) {// needs to be inside organelle
	    return false;
	} else {
	    for (int dx = -1; dx <= 1; dx++) {
		for (int dy = -1; dy <= 1; dy++) {
		    for (int dz = -1; dz <= 1; dz++) {
			if (!(dx == 0 && dy == 0 && dz == 0)) {
			    final long testPosition[] = { position[0] + dx, position[1] + dy, position[2] + dz };
			    sourceRandomAccess.setPosition(testPosition);
			    if (sourceRandomAccess.get().getIntegerLong() != referenceVoxelValue) {
				sourceRandomAccess.setPosition(position);
				return true;
			    }
			}
		    }
		}
	    }
	    sourceRandomAccess.setPosition(position);
	    return false;
	}

    }

    public static <T extends IntegerType<T>> boolean isSurfaceVoxel(final RandomAccess<T> raObject,
	    final RandomAccess<T> raSurface, long[] position) {
	raObject.setPosition(position);
	if (raObject.get().getIntegerLong() > 0) {// needs to be inside organelle
	    raSurface.setPosition(position);
	    if (raSurface.get().getIntegerLong() > 0) {
		return true;
	    }
	}
	return false;
    }

    public static void setupSparkAndCalculateContactSites(String inputN5Path, String outputN5Path, String inputN5DatasetName, String originalInputPairs, double contactDistance, double minimumVolumeCutoff, boolean doSelfContacts, boolean skipGeneratingContactBoundaries, boolean doNaiveMethod) throws IOException {
	final SparkConf conf = new SparkConf().setAppName("SparkContactSites");

	// Get all organelles
	String[] organelles = { "" };

	List<String[]> customOrganellePairs = new ArrayList<String[]>();
	if (originalInputPairs != null) {
	    String[] inputPairs = originalInputPairs.split(",");
	    HashSet<String> organelleSet = new HashSet<String>();
	    for (int i = 0; i < inputPairs.length; i++) {
		String organelle1 = inputPairs[i].split("_to_")[0];
		String organelle2 = inputPairs[i].split("_to_")[1];
		organelleSet.add(organelle1);
		organelleSet.add(organelle2);
		customOrganellePairs.add(new String[] { organelle1, organelle2 });
	    }
	    organelles = organelleSet.toArray(organelles);
	} else {
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

	}
	// First calculate the object contact boundaries
	for (String organelle : organelles) {
	    File contactBoundary = new File(
		    outputN5Path + "/" + organelle + "_contact_boundary_temp_to_delete");
	    File contactBoundaryPairs = new File(
		    outputN5Path + "/" + organelle + "_pairs_contact_boundary_temp_to_delete");
	    boolean skipGeneratingCurrentContactBoundaries = skipGeneratingContactBoundaries
		    && contactBoundary.isDirectory()
		    && (doSelfContacts ? contactBoundaryPairs.isDirectory() : true);
	    if (!skipGeneratingCurrentContactBoundaries) {

		JavaSparkContext sc = new JavaSparkContext(conf);
		List<BlockInformation> blockInformationList = BlockInformation
			.buildBlockInformationList(inputN5Path, organelle);
		calculateObjectContactBoundaries(sc, inputN5Path, organelle, outputN5Path,
			contactDistance, doSelfContacts, blockInformationList);
		sc.close();
	    }
	}
	System.out.println("finished boundaries");

	boolean doLM = false;
	if (customOrganellePairs.size() > 0) {
	    for (String[] customOrganellePair : customOrganellePairs) {
		calculateContactSites(conf, customOrganellePair, doSelfContacts,
			minimumVolumeCutoff, contactDistance, doLM, inputN5Path,
			outputN5Path, doNaiveMethod);
	    }
	} else {
	    calculateContactSites(conf, organelles, doSelfContacts, minimumVolumeCutoff,
		    contactDistance, doLM, inputN5Path, outputN5Path,
		    doNaiveMethod);
	}
	System.out.println("finished contact sites");
    }

    /**
     * Perform entire contact site analysis: generating contact boundaries and
     * finding contact site connected components.
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
	String outputN5Path = options.getOutputN5Path();
	String inputN5DatasetName = options.getInputN5DatasetName();
	String originalInputPairs = options.getInputPairs();
	double contactDistance = options.getContactDistance();
	double minimumVolumeCutoff = options.getMinimumVolumeCutoff();
	boolean doSelfContacts = options.getDoSelfContacts();
	boolean skipGeneratingContactBoundaries = options.getSkipGeneratingContactBoundaries();
	boolean doNaiveMethod = options.getDoNaiveMethod();
	
	setupSparkAndCalculateContactSites(inputN5Path, outputN5Path, inputN5DatasetName, originalInputPairs, contactDistance, minimumVolumeCutoff, doSelfContacts,skipGeneratingContactBoundaries,doNaiveMethod);
	
    }
}
