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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.Bressenham3D;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.roi.labeling.ImgLabeling;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Threshold a prediction but label it using another segmented volume's ids.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkRibosomeConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "Path to input N5")
		private String inputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = true, usage = "N5 Dataset name")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
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

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}

	}

	/**
	 * Method that relabels predictions above a certain threshold with the connected component object ID they are within.
	 * 
	 * @param sc								Spark context
	 * @param predictionN5Path					N5 path to predictions
	 * @param predictionDatasetName				Name of predictions
	 * @param connectedComponentsN5Path			N5 path to connected components
	 * @param connectedComponentsDatasetName	Name of connected components
	 * @param outputN5Path						N5 path to output
	 * @param thresholdIntensityCutoff			Threshold intensity cutoff
	 * @param blockInformationList				List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final void getRibosomeConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName, final String outputN5Path, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data sets.
		final N5Reader predictionN5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = predictionN5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(predictionN5Reader, inputN5DatasetName);
		
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		final String outputCentersDatasetName = inputN5DatasetName+"_centers";
		n5Writer.createGroup(outputCentersDatasetName);
		n5Writer.createDataset(outputCentersDatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputCentersDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		n5Writer.setAttribute(outputCentersDatasetName, "offset", predictionN5Reader.getAttribute(inputN5DatasetName, "offset", int[].class));

		final String outputSpheresDatasetName = inputN5DatasetName+"_spheres";
		n5Writer.createGroup(outputSpheresDatasetName);
		n5Writer.createDataset(outputSpheresDatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputSpheresDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		n5Writer.setAttribute(outputSpheresDatasetName, "offset", predictionN5Reader.getAttribute(inputN5DatasetName, "offset", int[].class));

		double ribosomeRadiusInNm = 10.0;
		double [] ribosomeRadiusInVoxels = 	new double[] {ribosomeRadiusInNm/pixelResolution[0],ribosomeRadiusInNm/pixelResolution[0],ribosomeRadiusInNm/pixelResolution[0]}; //gives 3 pixel sigma at 4 nm resolution

		
		// Do the labeling, parallelized over blocks
		final JavaRDD<BlockInformation> getAllLocalMaximaRDD = sc.parallelize(blockInformationList);
		JavaRDD<Map<Integer, List<long[]>>> allLocalMaximaRDD = getAllLocalMaximaRDD.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			//double [] sigma = new double[] {sigmaNm/pixelResolution[0],sigmaNm/pixelResolution[0],sigmaNm/pixelResolution[0]}; //gives 3 pixel sigma at 4 nm resolution
			//int[] sizes = Gauss3.halfkernelsizes( sigma );
			//long padding = (long) (sizes[0]+2*Math.ceil(ribosomeRadiusInVoxels[0])+3);//add extra padding so if a ribosome appears in adjacent block, will be correctly shown in current block
			long separation = Math.round(ribosomeRadiusInVoxels[0]);
			long padding =  separation+1;//separation*(paddedSeparation*paddedSeparation*paddedSeparation+2);//separation^3 since that is how we later split things up later, into independent sets where each is separated by separation in all 3 dimensions;*separation for max distance one will be affected by others. +2 for extra padding.//id3*separation;
			long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
			
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			/*RandomAccessibleInterval<DoubleType> rawPredictions = Views.offsetInterval(Views.extendMirrorSingle(
					(RandomAccessibleInterval<DoubleType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
					),paddedOffset, paddedDimension); */
			
			RandomAccessibleInterval<UnsignedByteType> rawPredictions = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
					),paddedOffset, paddedDimension);
			Img< FloatType > spherenessFraction = new ArrayImgFactory<FloatType>(new FloatType()).create(paddedDimension);
			getSpherenessFraction(rawPredictions.randomAccess(),spherenessFraction.randomAccess(), ribosomeRadiusInVoxels[0],paddedDimension, paddedOffset);
			Map<Integer, List<long[]>> allLocalMaximaInBlock = findAllLocalMaxima(spherenessFraction, 0, separation, padding, paddedOffset);
			return allLocalMaximaInBlock;
		});

		Broadcast<Map<Integer, List<long[]>>> broadcastedAllLocalMaxima = sc.broadcast(
				allLocalMaximaRDD.reduce((a,b) -> {
				for(Integer key : a.keySet()) {
					 List<long[]> bSet = b.getOrDefault(key, new ArrayList<long[]>());
					 bSet.addAll(a.get(key));
					 b.put(key,bSet);
				}
				return b;
			})
		);
		
		System.out.println("done with getting all local maxima");
		final JavaRDD<BlockInformation> createRibosomeSegmentationRDD = sc.parallelize(blockInformationList);
		createRibosomeSegmentationRDD.foreach(currentBlockInformation -> {
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			//double [] sigma = new double[] {sigmaNm/pixelResolution[0],sigmaNm/pixelResolution[0],sigmaNm/pixelResolution[0]}; //gives 3 pixel sigma at 4 nm resolution
			//int[] sizes = Gauss3.halfkernelsizes( sigma );
			//long padding = (long) (sizes[0]+2*Math.ceil(ribosomeRadiusInVoxels[0])+3);//add extra padding so if a ribosome appears in adjacent block, will be correctly shown in current block
			long separation = Math.round(ribosomeRadiusInVoxels[0]);
			long paddedSeparation = separation+1;
			long padding =  separation*(paddedSeparation*paddedSeparation*paddedSeparation+2);//separation^3 since that is how we later split things up later, into independent sets where each is separated by separation in all 3 dimensions;*separation for max distance one will be affected by others. +2 for extra padding.//id3*separation;
			long [] paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			long [] paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
			
			Map<Integer, List<long[]>> allLocalMaxima = broadcastedAllLocalMaxima.value();
			long [] paddedEnd = new long[] {paddedOffset[0]+paddedDimension[0],paddedOffset[1]+paddedDimension[1],paddedOffset[2]+paddedDimension[2]};
			
			Map<Integer, List<long[]>> currentLocalMaxima = new HashMap<Integer,List<long[]>>();
			//long tic = System.currentTimeMillis();
			for(Entry<Integer, List<long[]>> currentEntry : allLocalMaxima.entrySet()) {
				List<long[]> maximaForIndependentIndex = new ArrayList<long []>();
				Integer independentIndex = currentEntry.getKey();
				for(long[] overallPosition : currentEntry.getValue()) {
					if ( overallPosition[0]>=paddedOffset[0] && overallPosition[0]<paddedEnd[0] &&
							overallPosition[1]>=paddedOffset[1] && overallPosition[1]<paddedEnd[1] &&
									overallPosition[2]>=paddedOffset[2] && overallPosition[2]<paddedEnd[2]) {
						maximaForIndependentIndex.add(overallPosition);
					}	
				}
				currentLocalMaxima.put(independentIndex,maximaForIndependentIndex);
			}
			List<long[]> nonoverlappingMaxima = getNonoverlappingMaxima(currentLocalMaxima, (int)(paddedSeparation*paddedSeparation*paddedSeparation), separation);
			//System.out.println(System.currentTimeMillis()-tic);
			
			//for writing out, only need to make sure nearest centers are included
			padding = separation+1;//times 2 to allow for spheres
			paddedOffset = new long [] {offset[0]-padding,offset[1]-padding,offset[2]-padding};
			paddedDimension = new long [] {dimension[0]+2*padding,dimension[1]+2*padding,dimension[2]+2*padding};
			Img< UnsignedLongType > outputCenters = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(paddedDimension);
	        Img< UnsignedLongType > outputSpheres = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType()).create(paddedDimension);
	        plotCentersAndSpheres(nonoverlappingMaxima, outputCenters.randomAccess(), outputSpheres.randomAccess(), ribosomeRadiusInVoxels[0], paddedOffset, paddedDimension, outputDimensions);
	       //findAndDisplayLocalMaxima(sphereness, outputCenters.randomAccess(), outputSpheres.randomAccess(), ribosomeRadiusInVoxels[0], paddedOffset, paddedDimension, outputDimensions);
			
			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(Views.offsetInterval(outputCenters, new long[] {padding,padding,padding}, dimension), n5WriterLocal, outputCentersDatasetName, gridBlock[2]);
			N5Utils.saveBlock(Views.offsetInterval(outputSpheres, new long[] {padding,padding,padding}, dimension), n5WriterLocal, outputSpheresDatasetName, gridBlock[2]);

		});

	}

	public static List<long[]> getNonoverlappingMaxima(Map<Integer, List<long[]>> allLocalMaxima, int numIndependentIndices, long separation) {
		List<long[]> nonoverlappingMaxima = new ArrayList<long[]>();
		long separationSquared = separation * separation;
		for (Integer independentIndex=0; independentIndex<=numIndependentIndices;independentIndex++ ) {
			ArrayList<long[]> centersToAdd = new ArrayList<long[]>();
			List<long[]> independentLocalMaxima = allLocalMaxima.getOrDefault(independentIndex,new ArrayList<long[]>());
			for (long[] currentLocalMaxima : independentLocalMaxima) {

				boolean isNonoverlapping = true;
				for (long[] currentNonoverlappingMaxima : nonoverlappingMaxima) {
					long dx = currentLocalMaxima[0] - currentNonoverlappingMaxima[0];
					long dy = currentLocalMaxima[1] - currentNonoverlappingMaxima[1];
					long dz = currentLocalMaxima[2] - currentNonoverlappingMaxima[2];
					if (dx * dx + dy * dy + dz * dz <= separationSquared) {
						isNonoverlapping = false;
						break;
					}
				}
				if (isNonoverlapping) {
					centersToAdd.add(currentLocalMaxima);
				}
			}

			nonoverlappingMaxima.addAll(centersToAdd);
		}
		return nonoverlappingMaxima;
	}

	public static <T extends RealType<T>> Map<Integer, List<long[]>> findAllLocalMaxima(RandomAccessibleInterval<T> rai,
			double cutoff, long separation, long padding, long[] paddedOffset) {
		HashMap<Integer, List<long[]>> allLocalMaxima = new HashMap<Integer, List<long[]>>();
		RandomAccess<T> raiRA = rai.randomAccess();

		Set<List<Long>> spherePoints = getSpherePoints(separation);

		for (long x = padding; x < rai.dimension(0) - padding; x++) {
			for (long y = padding; y < rai.dimension(1) - padding; y++) {
				for (long z = padding; z < rai.dimension(2) - padding; z++) {
					long[] centerPosition = new long[] { x, y, z };
					raiRA.setPosition(centerPosition);
					double centerValue = raiRA.get().getRealDouble();
					/*
					 * long [] tempoverallCenterPosition = new long[]
					 * {x+paddedOffset[0],y+paddedOffset[1],z+paddedOffset[2]};
					 * if((tempoverallCenterPosition[0]==237 || tempoverallCenterPosition[0]==238)
					 * && tempoverallCenterPosition[1]==285 && tempoverallCenterPosition[2]==358) {
					 * System.out.println(Arrays.toString(paddedOffset)+" "
					 * +tempoverallCenterPosition[0]+" "+centerValue); }
					 */
					if (centerValue > cutoff) {
						boolean isMaximum = true;
						for (List<Long> delta : spherePoints) {
							raiRA.setPosition(new long[] { x + delta.get(0), y + delta.get(1), z + delta.get(2) });
							if (raiRA.get().getRealDouble() > centerValue) {
								isMaximum = false;
								break;
							}

						}
						if (isMaximum) {
							long[] overallCenterPosition = new long[] { x + paddedOffset[0], y + paddedOffset[1],
									z + paddedOffset[2] };
							long paddedSeparation = separation + 1;
							int independentIndex = (int) (overallCenterPosition[0] % paddedSeparation
									+ (overallCenterPosition[1] % paddedSeparation) * paddedSeparation
									+ (overallCenterPosition[2] % paddedSeparation) * Math.pow(paddedSeparation, 2));
							/*
							 * so for instance, for paddedSeparation =4, a 16 voxel slice is split up like:
							 * 0 1 2 3 
							 * 4 5 6 7 
							 * 8 9 10 11 
							 * 12 13 14 15 
							 * meaning that each "0" will be
							 * independent of all others. if just use separation (without padding), then 0s
							 * will be dependent on other 0s etc since they will be padding apart //[237,
							 * 285, 358] //[238, 285, 358] /*if(overallCenterPosition[0]==238 &&
							 * (overallCenterPosition[1]==151 || overallCenterPosition[1]==152) &&
							 * overallCenterPosition[2]==178) {
							 * System.out.println(Arrays.toString(paddedOffset)+" "+overallCenterPosition[1]
							 * +" "+independentIndex); }
							 */

							List<long[]> currentLocalMaxima = allLocalMaxima.getOrDefault(independentIndex,
									new ArrayList<long[]>());
							currentLocalMaxima.add(overallCenterPosition);
							allLocalMaxima.put(independentIndex, currentLocalMaxima);
						}

					}

				}
			}
		}
		return allLocalMaxima;
	}

	public static Set<List<Long>> keepNonOverlappingSpheres(Set<List<Long>> centers, int centerSeparationInVoxels,
			long[] paddedOffset) {
		Set<List<Long>> keptCenters = new HashSet<List<Long>>();
		// simpleBorderPoints.get((x+paddedOffset[0])%centerSeparationInVoxels+((y+paddedOffset[1])%centerSeparationInVoxels)*centerSeparationInVoxels+((z+paddedOffset[2])%centerSeparationInVoxels)*Math.pow(centerSeparationInVoxels,2)).add(index);

		return centers;
	}

	public static void doWatershedding(RandomAccessibleInterval<UnsignedByteType> ra) {
		// https://forum.image.sc/t/watershed-segmentation-using-ops/24713/3
		// Thresold the Output mask from fiji (where 0 is false and 255 is true)
		final RandomAccessibleInterval<NativeBoolType> mask = Converters.convert(ra, (a, b) -> {
			b.set(a.getIntegerLong() >= 127);
		}, new NativeBoolType());
		// Fill the holes
		// maskFilled = ij.op().morphology().fillHoles(maskBitType);

		// Perform the watershed
		boolean useEightConnectivity = true;
		boolean drawWatersheds = false;
		double sigma = 2.0;
		net.imagej.ImageJ ij = new net.imagej.ImageJ();
		ImgLabeling<Integer, IntType> watershedImgLabeling = ij.op().image().watershed(null, mask, useEightConnectivity,
				drawWatersheds, sigma, mask);

		ImageJ temp = new ij.ImageJ();
		// Display the result
		RandomAccessibleInterval<IntType> watershedImg = watershedImgLabeling.getIndexImg();
		ImagePlus watershedImgLabelingImp = ImageJFunctions.wrap(watershedImg, "wrapped");
		watershedImgLabelingImp.show();

	}

	public static boolean voxelPathExitsObject(RandomAccess<UnsignedByteType> ra, List<long[]> voxelsToCheck) {

		for (int i = 0; i < voxelsToCheck.size() - 1; i++) {// don't check start and end since those are defined already
															// to be in object
			long[] currentVoxel = voxelsToCheck.get(i);
			ra.setPosition(currentVoxel);
			if (ra.get().get() < 127)
				return true;
		}
		return false;
	}

	public static void getSpherenessFraction(RandomAccess<UnsignedByteType> rawPredictionsRA,
			RandomAccess<FloatType> spherenessFractionRA, double radius, long[] dimensions, long[] paddedOffset) {
		// updated to fix floating point rounding errors

		Set<List<Long>> spherePoints = getSpherePoints(radius);

		for (int x = 0; x < dimensions[0]; x++) {
			for (int y = 0; y < dimensions[1]; y++) {
				for (int z = 0; z < dimensions[2]; z++) {

					long[] sphereCenter = new long[] { x, y, z };
					rawPredictionsRA.setPosition(sphereCenter);
					if (rawPredictionsRA.get().get() >= 127) {// center is greater than 127
						int countForSphereCenteredAtxyz = 0;
						/*
						 * int comX = 0; int comY = 0; int comZ = 0;
						 */
						int deltaX = 0;
						int deltaY = 0;
						int deltaZ = 0;
						for (List<Long> currentSpherePoint : spherePoints) {
							long[] sphereCoordinate = new long[] { x + currentSpherePoint.get(0),
									y + currentSpherePoint.get(1), z + currentSpherePoint.get(2) };
							if (sphereCoordinate[0] >= 0 && sphereCoordinate[1] >= 0 && sphereCoordinate[2] >= 0
									&& sphereCoordinate[0] < dimensions[0] && sphereCoordinate[1] < dimensions[1]
									&& sphereCoordinate[2] < dimensions[2]) {
								rawPredictionsRA.setPosition(sphereCoordinate);
								if (rawPredictionsRA.get().get() >= 127 && !voxelPathExitsObject(rawPredictionsRA,
										Bressenham3D.getLine(sphereCenter, sphereCoordinate))) {
									countForSphereCenteredAtxyz++;
									deltaX += (x - sphereCoordinate[0]);
									deltaY += (y - sphereCoordinate[1]);
									deltaZ += (z - sphereCoordinate[2]);
									/*
									 * comX+=sphereCoordinate[0]+paddedOffset[0];//need to add paddedOffset,
									 * otherwise had floating point error comY+=sphereCoordinate[1]+paddedOffset[1];
									 * comZ+=sphereCoordinate[2]+paddedOffset[2];
									 */
								}
							}
						}
						/*
						 * float deltaCOMX =
						 * deltaX/(float)countForSphereCenteredAtxyz;//(x+paddedOffset[0])-comX/(float)
						 * countForSphereCenteredAtxyz; float deltaCOMY =
						 * deltaY/(float)countForSphereCenteredAtxyz;////(y+paddedOffset[1])-comY/(float
						 * )countForSphereCenteredAtxyz; float deltaCOMZ =
						 * deltaZ/(float)countForSphereCenteredAtxyz;////(z+paddedOffset[2])-comZ/(float
						 * )countForSphereCenteredAtxyz;
						 */
						float deltaCOM = (float) Math.sqrt((deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ)
								/ ((float) countForSphereCenteredAtxyz * countForSphereCenteredAtxyz));
						// float deltaCOM = (float)
						// Math.sqrt(deltaCOMX*deltaCOMX+deltaCOMY*deltaCOMY+deltaCOMZ*deltaCOMZ);
						float deltaCOMmetric = (float) (1 - deltaCOM / radius);
						// if(countForSphereCenteredAtxyz>spherePoints.size()/10) {
						spherenessFractionRA.setPosition(new long[] { x, y, z });
						spherenessFractionRA.get().set(countForSphereCenteredAtxyz + deltaCOMmetric);
						// }

						/*
						 * long [] overallCenterPosition = new long[]
						 * {x+paddedOffset[0],y+paddedOffset[1],z+paddedOffset[2]};
						 * 
						 * if(overallCenterPosition[0]==238 && (overallCenterPosition[1]==151 ||
						 * overallCenterPosition[1]==152) && overallCenterPosition[2]==178) {
						 * System.out.println(Arrays.toString(overallCenterPosition)+" "+deltaX+" "
						 * +deltaY+" "+deltaZ+" "+deltaCOM+" "+deltaCOMmetric+" "
						 * +countForSphereCenteredAtxyz); //[171, 171, 351] 0.44444275 -0.41666412
						 * 0.27777767 //[171, 171, 171] 0.44444275 -0.41666412 0.277771 //
						 * deltaX/(float)countForSphereCenteredAtxyz; // //
						 * deltaX/(float)countForSphereCenteredAtxyz; //
						 * 
						 * }
						 */
					}
				}
			}
		}
	}

	/**
	 * Checks all pixels in the image if they are a local maxima and labels the
	 * centers and creates sphere around them
	 * 
	 * @param <T>
	 * @param <U>
	 * @param source                 Source interval for finding maxima
	 * @param centersRA              Random accessible for storing maxima centers
	 * @param spheresRA              Random accessible for storing maxima spheres
	 * @param ribosomeRadiusInVoxels Radius of ribosomes in voxels
	 * @param paddedOffset           Padded offset
	 * @param paddedDimension        Padded dimensions
	 * @param overallDimensions      Overall dimensions of image
	 */
	public static <T extends RealType<T>, U extends IntegerType<U>, V extends IntegerType<V>> void findAndDisplayLocalMaxima(
			RandomAccessibleInterval<T> source, RandomAccess<U> centersRA, RandomAccess<U> spheresRA,
			double ribosomeRadiusInVoxels, long paddedOffset[], long paddedDimension[], long overallDimensions[]) {
		// define an interval that is one pixel smaller on each side in each dimension,
		// so that the search in the 8-neighborhood (3x3x3...x3) never goes outside
		// of the defined interval
		Interval interval = Intervals.expand(source, -1);

		// create a view on the source with this interval
		source = Views.interval(source, interval);

		// create a Cursor that iterates over the source and checks in a 8-neighborhood
		// if it is a minima
		final Cursor<T> center = Views.iterable(source).cursor();

		// instantiate a RectangleShape to access rectangular local neighborhoods
		// of radius 1 (that is 3x3x...x3 neighborhoods), skipping the center pixel
		// (this corresponds to an 8-neighborhood in 2d or 26-neighborhood in 3d, ...)
		final RectangleShape shape = new RectangleShape(1, true);

		// iterate over the set of neighborhoods in the image
		for (final Neighborhood<T> localNeighborhood : shape.neighborhoods(source)) {
			// what is the value that we investigate?
			// (the center cursor runs over the image in the same iteration order as
			// neighborhood)
			final T centerValue = center.next();
			if (centerValue.getRealDouble() > 0) {// Then is above threshold
				// keep this boolean true as long as no other value in the local neighborhood
				// is larger or equal
				boolean isMaximum = true;

				// check if all pixels in the local neighborhood that are smaller
				for (final T value : localNeighborhood) {
					// test if the center is smaller than the current pixel value
					if (centerValue.compareTo(value) < 0) {
						isMaximum = false;
						break;
					}
				}

				if (isMaximum) {
					long[] centerPosition = new long[] { center.getLongPosition(0), center.getLongPosition(1),
							center.getLongPosition(2) };// add 1 because of -1 earlier
					long[] overallCenterPosition = { centerPosition[0] + paddedOffset[0],
							centerPosition[1] + paddedOffset[1], centerPosition[2] + paddedOffset[2] };

					if (overallCenterPosition[0] >= 0 && overallCenterPosition[1] >= 0 && overallCenterPosition[2] >= 0
							&& overallCenterPosition[0] < overallDimensions[0]
							&& overallCenterPosition[1] < overallDimensions[1]
							&& overallCenterPosition[2] < overallDimensions[2]) {
						long setValue = ProcessingHelper.convertPositionToGlobalID(overallCenterPosition,
								overallDimensions);

						// set center to setValue
						centersRA.setPosition(centerPosition);
						centersRA.get().setInteger(setValue);

						// draw a sphere of radius one in the new image
						fillSphereWithValue(spheresRA, centerPosition, ribosomeRadiusInVoxels, setValue,
								paddedDimension);
					}
				}
			}
		}
	}

	public static void plotCentersAndSpheres(List<long[]> localMaxima, RandomAccess<UnsignedLongType> centersRA,
			RandomAccess<UnsignedLongType> spheresRA, double ribosomeRadiusInVoxels, long[] paddedOffset,
			long[] paddedDimension, long[] overallDimensions) {
		
		for (long[] overallCenterPosition : localMaxima) {
			long[] centerPosition = new long [] {overallCenterPosition[0]-paddedOffset[0], overallCenterPosition[1]-paddedOffset[1],overallCenterPosition[2]-paddedOffset[2]};
			if (overallCenterPosition[0] >= 0 && overallCenterPosition[1] >= 0 && overallCenterPosition[2] >= 0
					&& overallCenterPosition[0] < overallDimensions[0]
					&& overallCenterPosition[1] < overallDimensions[1]
					&& overallCenterPosition[2] < overallDimensions[2]) {
				if(centerPosition[0]>=0 && centerPosition[0]<paddedDimension[0] && 
						centerPosition[1]>=0 && centerPosition[1]<paddedDimension[1] &&
						centerPosition[2]>=0 && centerPosition[2]<paddedDimension[2]){
					long setValue = ProcessingHelper.convertPositionToGlobalID(overallCenterPosition, overallDimensions);
	
					/*
					 * if(setValue==51986579 || setValue== 51987119) {
					 * System.out.println(Arrays.toString(overallCenterPosition)); //[237, 285, 358]
					 * //[238, 285, 358] }
					 */
					// set center to setValue
					centersRA.setPosition(centerPosition);
					centersRA.get().setInteger(setValue);
	
					// draw a sphere of radius one in the new image
					fillSphereWithValue(spheresRA, centerPosition, ribosomeRadiusInVoxels, setValue, paddedDimension);
				}
			}
		}
	}

	public static <T extends IntegerType<T>> void fillSphereWithValue(RandomAccess<T> ra, long[] center, double radius,
			long setValue, long[] dimensions) {
		long radiusCeiling = (long) Math.ceil(radius);
		double radiusSquared = radius * radius;
		for (long dx = -radiusCeiling; dx <= radiusCeiling; dx++) {
			for (long dy = -radiusCeiling; dy <= radiusCeiling; dy++) {
				for (long dz = -radiusCeiling; dz <= radiusCeiling; dz++) {
					if (dx * dx + dy * dy + dz * dz <= radiusSquared) {
						long[] positionInSphere = new long[] { center[0] + dx, center[1] + dy, center[2] + dz };
						if (positionInSphere[0] >= 0 && positionInSphere[1] >= 0 && positionInSphere[2] >= 0
								&& positionInSphere[0] < dimensions[0] && positionInSphere[1] < dimensions[1]
								&& positionInSphere[2] < dimensions[2]) {
							ra.setPosition(positionInSphere);
							ra.get().setInteger(setValue);
						}
					}
				}
			}
		}
	}

	public static Set<List<Long>> getSpherePoints(double radius) {
		Set<List<Long>> pointsInSphere = new HashSet<List<Long>>();
		long radiusCeiling = (long) Math.ceil(radius);
		double radiusSquared = radius * radius;
		for (long dx = -radiusCeiling; dx <= radiusCeiling; dx++) {
			for (long dy = -radiusCeiling; dy <= radiusCeiling; dy++) {
				for (long dz = -radiusCeiling; dz <= radiusCeiling; dz++) {
					if (dx * dx + dy * dy + dz * dz <= radiusSquared) {
						pointsInSphere.add(Arrays.asList(dx, dy, dz));
					}
				}
			}
		}
		return pointsInSphere;
	}

	/**
	 * Take input arguments and produce ribosome connected components
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

		final SparkConf conf = new SparkConf().setAppName("SparkRibosomeConnectedComponents");

		// Create block information list
		List<BlockInformation> blockInformationList = BlockInformation
				.buildBlockInformationList(options.getInputN5Path(), "ribosomes");
		
		// Run connected components
		JavaSparkContext sc = new JavaSparkContext(conf);
		getRibosomeConnectedComponents(sc, options.getInputN5Path(), options.getInputN5DatasetName(),
				options.getOutputN5Path(), blockInformationList);
		sc.close();

	}
}
