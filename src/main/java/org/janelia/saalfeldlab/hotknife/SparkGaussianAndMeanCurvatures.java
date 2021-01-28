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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.decomposition.eig.SymmetricQRAlgorithmDecomposition_DDRM;
import org.janelia.saalfeldlab.hotknife.ops.GradientCenter;
import org.janelia.saalfeldlab.hotknife.ops.SimpleGaussRA;
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

import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Calculate curvature for a dataset Borrowed from
 * https://github.com/saalfeldlab/hot-knife/blob/tubeness/src/test/java/org/janelia/saalfeldlab/hotknife/LazyBehavior.java
 * 
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkGaussianAndMeanCurvatures {
//    @SuppressWarnings("serial")
//    public static class Options extends AbstractOptions implements Serializable {
//
//	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/data.n5.")
//	private String inputN5Path = null;
//
//	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/data.n5")
//	private String outputN5Path = null;
//
//	@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle. Requires organelle_medialSurface as well.")
//	private String inputN5DatasetName = null;
//
//	@Option(name = "--scaleSteps", required = false, usage = "N5 dataset, e.g. organelle. Requires organelle_medialSurface as well.")
//	private int scaleSteps = 12;
//
//	@Option(name = "--calculateSphereness", required = false, usage = "Calculate Sphereness; if not set, will calculate sheetness")
//	private boolean calculateSphereness = false;
//
//	public Options(final String[] args) {
//
//	    final CmdLineParser parser = new CmdLineParser(this);
//	    try {
//		parser.parseArgument(args);
//
//		if (outputN5Path == null)
//		    outputN5Path = inputN5Path;
//
//		parsedSuccessfully = true;
//	    } catch (final CmdLineException e) {
//		System.err.println(e.getMessage());
//		parser.printUsage(System.err);
//	    }
//	}
//
//	public String getInputN5Path() {
//	    return inputN5Path;
//	}
//
//	public String getInputN5DatasetName() {
//	    return inputN5DatasetName;
//	}
//
//	public String getOutputN5Path() {
//	    return outputN5Path;
//	}
//
//	public int getScaleSteps() {
//	    return scaleSteps;
//	}
//
//	public boolean getCalculateSphereness() {
//	    return calculateSphereness;
//	}
//
//    }
//
//    /**
//     * Compute curvatures for objects in images.
//     *
//     * Calculates the sheetness of objects in images at their medial surfaces.
//     * Repetitively smoothes image, stopping for a given medial surface voxel when
//     * the laplacian at that voxel is smallest. Then calculates sheetness based on
//     * all corresponding eigenvalues of hessian.
//     * 
//     * @param sc                   Spark context
//     * @param n5Path               Input N5 path
//     * @param inputDatasetName     Input N5 dataset name
//     * @param n5OutputPath         Output N5 path
//     * @param outputDatasetName    Output N5 dataset name
//     * @param scaleSteps           Number of scale steps
//     * @param calculateSphereness  If true, do sphereness; else do sheetness
//     * @param blockInformationList List of block information to parallize over
//     * @throws IOException
//     */
//    @SuppressWarnings("unchecked")
//    public static final <T extends IntegerType<T> & NativeType<T>> void computeCurvature(final JavaSparkContext sc,
//	    final String n5Path, final String inputDatasetName, final String n5OutputPath, String outputDatasetName,
//	    final List<BlockInformation> blockInformationList) throws IOException {
//
//	// Read in input block information
//	final N5Reader n5Reader = new N5FSReader(n5Path);
//	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
//	final long[] dimensions = attributes.getDimensions();
//	final int[] blockSize = attributes.getBlockSize();
//	final double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
//
//	// Create output
//	final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
//	String finalOutputDatasetName = "_gaussianCurvature";
//	n5Writer.createDataset(finalOutputDatasetName, dimensions, blockSize, DataType.FLOAT64, new GzipCompression());
//	n5Writer.setAttribute(finalOutputDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
//
//	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
//	rdd.foreach(blockInformation -> {
//	    // Get information for processing blocks
//	    final long[][] gridBlock = blockInformation.gridBlock;
//	    long[] offset = gridBlock[0];
//	    long[] dimension = gridBlock[1];
//
//	    double maxSigma = 0;
//	    int windowSizeParameter = 10;
//	    int halfCubeLength = 2;
//	    int padding = windowSizeParameter + 1;// cube that is 5x5x5
//	    long[] paddedOffset = new long[] { offset[0] - padding, offset[1] - padding, offset[2] - padding };
//	    long[] paddedDimension = new long[] { dimension[0] + 2 * padding, dimension[1] + 2 * padding,
//		    dimension[2] + 2 * padding };
//	    final N5Reader n5BlockReader = new N5FSReader(n5Path);
//	    // based on https://www.sciencedirect.com/science/article/pii/S1524070315000284
//
//	    // Step 1: create mask
//	    RandomAccessibleInterval<T> source = (RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader,
//		    inputDatasetName);
//	    RandomAccessibleInterval<UnsignedLongType> mask = getInternalGraidentUsingCube(source, halfCubeLength);
//
//	    // Step 2: get normals
//	    HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap = calculateNormalsAtSurfaceVoxels(source, mask,
//		    windowSizeParameter);
//
//	    // Step 3-7
//	    calculateCurvatures(source, surfaceVoxelsToNormalMap);
//
//	    // Step 4: calculate
//
//	});
//
//    }
//
//    private static <T extends IntegerType<T> & NativeType<T>> void calculateCurvatures(
//	    HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap) {
//	// calculate curvature at each surface voxel
//	float[] u = new float[3];
//	float[] v = new float[3];
//	float[] X_u = new float[3];
//	float[] X_v = new float[3];
//	float[] N_u = new float[3];
//	float[] N_v = new float[3];
//
//	ArrayList<int[]> neighborsToCheck = new ArrayList<int[]>();
//	neighborsToCheck.add(new int[] { -1, 0, 0 });
//	neighborsToCheck.add(new int[] { 1, 0, 0 });
//	neighborsToCheck.add(new int[] { 0, -1, 0 });
//	neighborsToCheck.add(new int[] { 0, 1, 0 });
//	neighborsToCheck.add(new int[] { 0, 0, -1 });
//	neighborsToCheck.add(new int[] { 0, 0, 1 });
//
//	FundamentalFormInformation f_u = new FundamentalFormInformation();
//	FundamentalFormInformation f_v = new FundamentalFormInformation();
//	for (Entry<List<Integer>, float[]> entry : surfaceVoxelsToNormalMap.entrySet()) {
//	    FundamentalFormInformation f = new FundamentalFormInformation();
//	    f.p = entry.getKey();
//	    f.normal = entry.getValue();
//
//	    // step 3: calculate tangent vectors, u and v
//	    calculateTangentVectors(f);
//
//	    // Step 4: get X_u and X_v
//	    getXs(f, surfaceVoxelsToNormalMap, neighborsToCheck);
//
//	    // Step 5: get N_u and N_v
//	    getNs(surfaceVoxel, u, v, surfaceVoxelsToNormalMap, N_u, N_v, neighborsToCheck);
//
//	}
//
//    }
//
//    public static class FundamentalFormInformation {
//	public float[] normal;
//	public List<Integer> p;
//	public FundamentalForm_x u;
//	public FundamentalForm_x v;
//	public HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap;
//	public float[] X_u;
//	public float[] X_v;
//	public float[] N_u;
//	public float[] N_v;
//	public float[][] I_1 = new float[2][2];
//	public float[][] I_2 = new float[2][2];
//	
//	FundamentalFormInformation(float[] normal, List<Integer> p, HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap) {
//	    this.normal = normal;
//	    this.p = p;
//	    this.surfaceVoxelsToNormalMap=surfaceVoxelsToNormalMap;
//	    
//	    float[] u = new float[3];
//	    float[] v = new float[3];
//	    this.calculateTangentVectors(u,v);
//	    
//	    List<List<Integer>> neighboringSurfaceVoxels = getNeighboringSurfaceVoxels();
//	    this.u = new FundamentalForm_x(normal, p, u, neighboringSurfaceVoxels, surfaceVoxelsToNormalMap);
//	    this.v = new FundamentalForm_x(normal, p, v, neighboringSurfaceVoxels, surfaceVoxelsToNormalMap);
//
//	}
//
//	private void calculateTangentVectors(float [] u, float [] v) {
//	    float[] sortedNormal = new float[] { this.normal[0], this.normal[1], this.normal[2] };
//	    Arrays.sort(sortedNormal);
//
//	    ArrayList<float[]> bs = new ArrayList<float[]>();
//	    bs.add(new float[] { 1, 0, 0 });
//	    bs.add(new float[] { 0, 1, 0 });
//	    bs.add(new float[] { 0, 0, 1 });
//
//	    int b_alpha_i = -1, b_beta_i = -1;
//	    for (int i = 0; i < 3; i++) {
//		if (this.normal[i] == sortedNormal[0] && b_alpha_i == -1) {
//		    b_alpha_i = i;
//		    break;
//		}
//		if (this.normal[i] == sortedNormal[1] && b_beta_i == -1) {
//		    b_beta_i = i;
//		    break;
//		}
//	    }
//
//	    float[] b_alpha = bs.get(b_alpha_i);
//	    float[] b_beta = bs.get(b_beta_i);
//
//	    float[] u_tilde = new float[3];
//	    float[] v_tilde = new float[3];
//	    float u_tilde_mag = 0, v_tilde_mag = 0;
//	    for (int i = 0; i < 3; i++) {
//		u_tilde[i] = b_alpha[i] - this.normal[b_alpha_i] * this.normal[i];
//		u_tilde_mag += u_tilde[i] * u_tilde[i];
//
//		v_tilde[i] = b_beta[i] - this.normal[b_beta_i] * this.normal[i];
//		v_tilde_mag += v_tilde[i] * v_tilde[i];
//	    }
//	    u_tilde_mag = (float) Math.sqrt(u_tilde_mag);
//	    v_tilde_mag = (float) Math.sqrt(v_tilde_mag);
//
//	    for (int i = 0; i < 3; i++) {
//		u[i] = u_tilde[i] / u_tilde_mag;
//		v[i] = v_tilde[i] / v_tilde_mag;
//	    }
//	    
//	}
//	
//        public List<List<Integer>> getNeighboringSurfaceVoxels() {
//            List<List<Integer>> neighboringSurfaceVoxels = new ArrayList<List<Integer>>();
//            for (int dx = -1; dx <= 1; dx++) {
//        	for (int dy = -1; dy <= 1; dy++) {
//        	    for (int dz = -1; dz <= 1; dz++) {
//        		if (!(dx == 0 && dy == 0 && dz == 0)) {// should we exclude current voxel?
//        		    List<Integer> possibleNeighbor = Arrays.asList(ArrayUtils.toObject(
//        			    new int[] { this.p.get(0) + dx, this.p.get(1) + dy, this.p.get(2) + dz }));
//        		    if (this.surfaceVoxelsToNormalMap.keySet().contains(possibleNeighbor)) {
//        			neighboringSurfaceVoxels.add(possibleNeighbor);
//        		    }
//        		}
//        	    }
//        	}
//            }
//            return neighboringSurfaceVoxels;
//        }
//        
//	public void getXs() {
//	    this.X_u = this.u.getX_x();
//	    this.X_v = this.v.getX_x();
//	}
//
//	public void getNs() {
//	    this.N_u = this.u.getN_x();
//	    this.N_v = this.v.getN_x();
//	}
//	
//	
//	public void getI_1(){
//	    this.I_1[0][0] = scalarProduct(this.X_u,this.X_u);
//	    this.I_1[0][1] = scalarProduct(this.X_u,this.X_v);
//	    this.I_1[1][0] = scalarProduct(this.X_v,this.X_u);
//	    this.I_1[1][1] = scalarProduct(this.X_v,this.X_v);
//	}
//	
//	public void getI_2(){
//	    this.I_2[0][0] = -scalarProduct(this.X_u,this.N_u);
//	    this.I_2[0][1] = -scalarProduct(this.X_u,this.N_v);
//	    this.I_2[1][0] = -scalarProduct(this.X_v,this.N_u);
//	    this.I_2[1][1] = -scalarProduct(this.X_v,this.N_v);
//	}
//	
//	public void getGaussianCurvature() {
//		http://web.cs.iastate.edu/~cs577/handouts/gaussian-curvature.pdf
//
//	}
//	public float scalarProduct (float[] v1, float[] v2) {
//	    float sp = 0;
//	    for(int d=0; d<3; d++) {
//		sp+=v1[d]*v2[d];
//	    }
//	    return sp;
//	}
//    }
//
//    public static class FundamentalForm_x {
//	    public float[] normal = new float[3];
//	    public List<Integer> p = new ArrayList<Integer>();
//	    public float[] x;
//	    public float[] p_x;
//	    public float[] p_plus_x;
//	    public List<Integer> nearestNeighborSurfaceVoxel_x;
//	    public HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap;
//	    public List<List<Integer>> neighboringSurfaceVoxels;
//
//	    FundamentalForm_x(float[] normal, List<Integer> p, float[] x,List<List<Integer>> neighboringSurfaceVoxels, HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap) {
//		this.normal = normal;
//		this.p = p;
//		this.x = x;
//		this.surfaceVoxelsToNormalMap=surfaceVoxelsToNormalMap;
//		this.neighboringSurfaceVoxels=neighboringSurfaceVoxels;
//	    }
//
//	    private float[] getX_x() {
//		float[] p_x = getP_x();
//		float[] X_x = { p_x[0] - this.p.get(0), p_x[1] - this.p.get(1), p_x[2] - this.p.get(2) };
//		return X_x;
//	    }
//
//	    private float[] getP_x() {
//		this.p_plus_x = new float[] { this.p.get(0) + this.x[0], this.p.get(1) + this.x[1], this.p.get(2) + this.x[2] };
//		float minDist = (float) 1E9;
//		float dX, dY, dZ, currentDist;
//		for (List<Integer> neighboringSurfaceVoxel : this.neighboringSurfaceVoxels) {
//		    dX = neighboringSurfaceVoxel.get(0) - p_plus_x[0];
//		    dY = neighboringSurfaceVoxel.get(1) - p_plus_x[1];
//		    dZ = neighboringSurfaceVoxel.get(2) - p_plus_x[2];
//		    currentDist = dX * dX + dY * dY + dZ * dZ;
//		    if (currentDist < minDist) {
//			minDist = currentDist;
//			this.nearestNeighborSurfaceVoxel_x = neighboringSurfaceVoxel;
//		    }
//		}
//
//		float[] normal = this.surfaceVoxelsToNormalMap.get(this.nearestNeighborSurfaceVoxel_x );
//		this.p_x = projectPointOntoPlane(p_plus_x, this.nearestNeighborSurfaceVoxel_x, normal);
//	    }
//
//	    private float[] projectPointOntoPlane(float[] p, List<Integer> o, float[] n) {
//		// https://stackoverflow.com/questions/9605556/how-to-project-a-point-onto-a-plane-in-3d
//		float[] v = { p[0] - o.get(0), p[1] - o.get(1), p[2] - o.get(2) };
//		float[] d = { v[0] * n[0], v[1] * n[1], v[2] * n[2] };
//		float[] projection = { p[0] - d[0] * n[0], p[1] - d[1] * n[1], p[2] - d[2] * n[2] };
//
//		return projection;
//	    }
//
//	    private float[] getN_x() {
//		float [] N_x = new float [3];
//		float [] N = weightedAverageOfNormals_p_plus_x();// not doing linear interpolation...
//		float [] N_x_start = projectPointOntoPlane(new float[] {0,0,0}, nearestNeighborSurfaceVoxel_x, this.surfaceVoxelsToNormalMap.get(nearestNeighborSurfaceVoxel_x));
//		float [] N_x_end = projectPointOntoPlane(N, nearestNeighborSurfaceVoxel_x, this.surfaceVoxelsToNormalMap.get(nearestNeighborSurfaceVoxel_x));
//		for(int d=0; d<3; d++) {
//		   N_x[d]=N_x_end[d]-N_x_start[d];
//		}
//		return N_x;
//	    }
//
//	    private float [] weightedAverageOfNormals_p_plus_x() {
//		//p+x seems dumb since it won't be on surface...so we'll use the voxels neighboring the voxel we projected onto
//		//int [] containingVoxel = new int[] {(int) Math.floor(this.p_plus_x[0]+0.5), (int) Math.floor(this.p_plus_x[1]+0.5), (int) Math.floor(this.p_plus_x[2]+0.5)};
//		float[] averageNormal = {0,0,0};
//		float[] sum_dist = {0,0,0};
//		for(int dx=-1; dx<=1; dx++) {
//		    for(int dy=-1; dy<=1; dy++) {
//			for(int dz=-1; dz<=1; dz++) {
//			    List<Integer> possibleSurfaceVoxel = Arrays.asList(nearestNeighborSurfaceVoxel_x.get(0)+dx,nearestNeighborSurfaceVoxel_x.get(1)+dy,nearestNeighborSurfaceVoxel_x.get(2)+dz);
//			    if (this.surfaceVoxelsToNormalMap.containsKey(possibleSurfaceVoxel)) {
//				float[] neighboringNormal = this.surfaceVoxelsToNormalMap.get(possibleSurfaceVoxel);
//				for (int d=0; d<3;d++) {
//				float dist = Math.abs(this.p_plus_x[d]-possibleSurfaceVoxel.get(d));
//				averageNormal[d]+=neighboringNormal[d]/dist;
//				sum_dist[d]+=1/dist;
//				}
//			    }
//			}
//		    }
//		}
//		for(int d=0; d<3; d++) {
//		    averageNormal[d]/=sum_dist[d];
//		}
//		
//		normalize(averageNormal);
//		return averageNormal;
//	    }
//	}
//
//	private static <T extends IntegerType<T> & NativeType<T>> RandomAccessibleInterval<UnsignedLongType> getInternalGraidentUsingCube(
//		RandomAccessibleInterval<T> source, int halfLength) {
//	    long[] dimensions = new long[3];
//	    long[] pos;
//
//	    source.dimensions(dimensions);
//	    RandomAccess<T> sourceRA = source.randomAccess();
//	    RandomAccessibleInterval<UnsignedLongType> mask = Views.offsetInterval(ArrayImgs.unsignedLongs(dimensions),
//		    new long[] { 0, 0, 0 }, dimensions);
//	    RandomAccess<UnsignedLongType> maskRA = mask.randomAccess();
//	    for (long x = halfLength; x < dimensions[0] - halfLength; x++) {
//		for (long y = halfLength; y < dimensions[1] - halfLength; y++) {
//		    for (long z = halfLength; z < dimensions[2] - halfLength; z++) {
//			pos = new long[] { x, y, z };
//			if (isInInternalGradient(sourceRA, pos, halfLength)) {
//
//			    maskRA.setPosition(pos);
//			    maskRA.get().set(1);
//			}
//		    }
//		}
//	    }
//
//	    return mask;
//	}
//
//	private static <T extends IntegerType<T> & NativeType<T>> HashMap<List<Integer>, float[]> calculateNormalsAtSurfaceVoxels(
//		RandomAccessibleInterval<T> source, RandomAccessibleInterval<UnsignedLongType> mask,
//		int windowSizeParameter) {
//	    RandomAccess<T> sourceRA = source.randomAccess();
//	    long[] dimensions = new long[3];
//	    int[] pos;
//	    // List<Integer> posList;
//
//	    int[] neighborPos;
//	    source.dimensions(dimensions);
//	    // neighbors: ‖(x,y,z)−(x′,y′,z′)‖∞≤1 from paper
//	    // TODO: if multiple objects in image
//	    // normals map:
//	    HashMap<List<Integer>, float[]> surfaceVoxelToNormalMap = new HashMap<List<Integer>, float[]>();
//
//	    for (int x = 1; x < dimensions[0] - 1; x++) {
//		for (int y = 1; y < dimensions[1] - 1; y++) {
//		    for (int z = 1; z < dimensions[2] - 1; z++) {
//			pos = new int[] { x, y, z };
//			sourceRA.setPosition(pos);
//			long referenceID = sourceRA.get().getIntegerLong();
//			if (referenceID > 0) {
//			    for (int dx = -1; dx <= 1; dx++) {
//				for (int dy = -1; dy <= 1; dy++) {
//				    for (int dz = -1; dz <= 1; dz++) {
//					if (!(dx == 0 && dy == 0 && dz == 0)) {
//					    neighborPos = new int[] { pos[0] + dx, pos[1] + dy, pos[2] + dz };
//					    sourceRA.setPosition(neighborPos);
//					    if (sourceRA.get().getIntegerLong() != referenceID) {
//						float[] normal = calculateNormal(mask, pos, referenceID,
//							windowSizeParameter);
//						// posList = Arrays.stream(pos).boxed().collect(Collectors.toList());
//						surfaceVoxelToNormalMap.put(Arrays.asList(ArrayUtils.toObject(pos)),
//							normal);
//						break;// is a surface voxel
//					    }
//					}
//				    }
//				}
//			    }
//			}
//		    }
//		}
//	    }
//	    return surfaceVoxelToNormalMap;
//	}
//
//	private static <T extends IntegerType<T> & NativeType<T>> float[] calculateNormal(
//		RandomAccessibleInterval<T> mask, int[] pos, long referenceID, int windowSizeParameter) {
//	    int com[] = { 0, 0, 0 };
//	    float count = 0;
//	    RandomAccess<T> maskRA = mask.randomAccess();
//	    for (int dx = -windowSizeParameter; dx <= windowSizeParameter; dx++) {
//		for (int dy = -windowSizeParameter; dy <= windowSizeParameter; dy++) {
//		    for (int dz = -windowSizeParameter; dz <= windowSizeParameter; dz++) {
//			maskRA.setPosition(new int[] { pos[0] + dx, pos[1] + dy, pos[2] + dz });
//			if (maskRA.get().getIntegerLong() == referenceID) {
//			    com[0] += dx;
//			    com[1] += dy;
//			    com[2] += dz;
//			    count++;
//			}
//		    }
//		}
//	    }
//
//	    float[] normal = new float[] { com[0] / count, com[1] / count, com[2] / count };
//	    // normalized:
//	    normalize(normal);
//
//	    return normal;
//	}
//	private static void normalize(float[] v) {
//	    double vMag = Math.sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2]);
//	    for (int i = 0; i < 3; i++) {
//		v[i] /= vMag;
//	    }
//	}
//
//	private static <T extends IntegerType<T> & NativeType<T>> boolean isInInternalGradient(RandomAccess<T> sourceRA,
//		long[] pos, int halfLength) {
//	    sourceRA.setPosition(pos);
//	    long[] newPos;
//	    if (sourceRA.get().getIntegerLong() == 0) {
//		return false;
//	    } else {
//		for (long dx = -halfLength; dx <= halfLength; dx++) {
//		    for (long dy = -halfLength; dy <= halfLength; dy++) {
//			for (long dz = -halfLength; dz <= halfLength; dz++) {
//			    newPos = new long[] { pos[0] + dx, pos[1] + dy, pos[2] + dz };
//			    sourceRA.setPosition(newPos);
//			    if (sourceRA.get().getIntegerLong() == 0) {
//				return true;
//			    }
//			}
//		    }
//		}
//	    }
//	    return false;
//	}
//
//	
//	
//
//	/**
//	 * Calculate sheetness given input args
//	 * 
//	 * @param args
//	 * @throws IOException
//	 * @throws InterruptedException
//	 * @throws ExecutionException
//	 */
//	public static final void main(final String... args)
//		throws IOException, InterruptedException, ExecutionException {
//
//	    final Options options = new Options(args);
//
//	    if (!options.parsedSuccessfully)
//		return;
//
//	    String inputN5DatasetName = options.getInputN5DatasetName();
//	    String inputN5Path = options.getInputN5Path();
//	    String outputN5Path = options.getOutputN5Path();
//	    int scaleSteps = options.getScaleSteps();
//	    boolean calculateSphereness = options.getCalculateSphereness();
//
//	    setupSparkAndCalculateCurvature(inputN5Path, inputN5DatasetName, outputN5Path, scaleSteps,
//		    calculateSphereness);
//
//	}
}
