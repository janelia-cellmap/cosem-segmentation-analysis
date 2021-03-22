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
import org.janelia.cosem.ops.GradientCenter;
import org.janelia.cosem.ops.SimpleGaussRA;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
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
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
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
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/data.n5.")
	private String inputN5Path = null;

	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/data.n5")
	private String outputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle. Requires organelle_medialSurface as well.")
	private String inputN5DatasetName = null;

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
     * Compute curvatures for objects in images.
     *
     * Calculates the sheetness of objects in images at their medial surfaces.
     * Repetitively smoothes image, stopping for a given medial surface voxel when
     * the laplacian at that voxel is smallest. Then calculates sheetness based on
     * all corresponding eigenvalues of hessian.
     * 
     * @param sc                   Spark context
     * @param n5Path               Input N5 path
     * @param inputDatasetName     Input N5 dataset name
     * @param n5OutputPath         Output N5 path
     * @param outputDatasetName    Output N5 dataset name
     * @param scaleSteps           Number of scale steps
     * @param calculateSphereness  If true, do sphereness; else do sheetness
     * @param blockInformationList List of block information to parallize over
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void calculateGaussianAndMeanCurvature(
	    final JavaSparkContext sc, final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    String outputDatasetName, final List<BlockInformation> blockInformationList) throws IOException {

	// Create output
	final String gaussianCurvatureDatasetName = inputDatasetName + "_gaussianCurvature";
	final String meanCurvatureDatasetName = inputDatasetName + "_meanCurvature";

	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, inputDatasetName, n5OutputPath,
		gaussianCurvatureDatasetName, DataType.FLOAT32);
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, inputDatasetName, n5OutputPath,
		meanCurvatureDatasetName, DataType.FLOAT32);

	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(blockInformation -> {
	    // Get information for processing blocks
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    int windowSizeParameter = 35;
	    int halfCubeLength = 10;
	    int padding = windowSizeParameter + halfCubeLength + 3;// cube that is 5x5x5
	    long[] paddedOffset = new long[] { offset[0] - padding, offset[1] - padding, offset[2] - padding };
	    long[] paddedDimension = new long[] { dimension[0] + 2 * padding, dimension[1] + 2 * padding,
		    dimension[2] + 2 * padding };
	    // based on https://www.sciencedirect.com/science/article/pii/S1524070315000284

	    // Step 1: create mask
	    RandomAccessibleInterval<T> source = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5Path,
		    inputDatasetName, paddedOffset, paddedDimension);
	    RandomAccessibleInterval<UnsignedLongType> mask = getInternalGraidentUsingCube(source, halfCubeLength);
	   /* new ImageJ();
	    ImageJFunctions.show(source);
	    ImageJFunctions.show(mask);*/
	    // Step 2: get normals
	    HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap = calculateNormalsAtSurfaceVoxels(source, mask,
		    windowSizeParameter);

	    // Step 3-7 at each voxel
	    RandomAccessibleInterval<FloatType> gaussianCurvature = ArrayImgs.floats(paddedDimension);
	    RandomAccess<FloatType> gaussianCurvatureRA = gaussianCurvature.randomAccess();

	    RandomAccessibleInterval<FloatType> meanCurvature = ArrayImgs.floats(paddedDimension);
	    RandomAccess<FloatType> meanCurvatureRA = meanCurvature.randomAccess();

	    for (Entry<List<Integer>, float[]> entry : surfaceVoxelsToNormalMap.entrySet()) {
		List<Integer> p = entry.getKey();
		if (p.get(0) >= padding && p.get(1) >= padding && p.get(2) >= padding
			&& p.get(0) < dimension[0] + padding && p.get(1) < dimension[1] + padding
			&& p.get(2) < dimension[2] + padding) {

		    float[] normal = entry.getValue();

		    CurvatureCalculator f = new CurvatureCalculator(normal, p, surfaceVoxelsToNormalMap, paddedOffset);
		    f.calculateCurvatures();

		    int[] pos = new int[] { p.get(0), p.get(1), p.get(2) };
		    gaussianCurvatureRA.setPosition(pos);
		    meanCurvatureRA.setPosition(pos);

		    gaussianCurvatureRA.get().set(f.gaussianCurvature);
		    meanCurvatureRA.get().set(f.meanCurvature);

		}

	    }
	    gaussianCurvature = Views.offsetInterval(gaussianCurvature, new long[] { padding, padding, padding },
		    dimension);
	    meanCurvature = Views.offsetInterval(meanCurvature, new long[] { padding, padding, padding }, dimension);

	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(gaussianCurvature, n5BlockWriter, gaussianCurvatureDatasetName, gridBlock[2]);
	    N5Utils.saveBlock(meanCurvature, n5BlockWriter, meanCurvatureDatasetName, gridBlock[2]);

	});

    }

    public static class CurvatureCalculator {
	public float[] normal;
	public List<Integer> p;
	public FundamentalForm_x u, v;
	public HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap;
	public float[] X_u, X_v, N_u, N_v;
	public float E, F, G, L, M1, M2, N;
	public float gaussianCurvature, meanCurvature;
	public long[] offset;

	CurvatureCalculator(float[] normal, List<Integer> p, HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap,
		long[] offset) {
	    this.normal = normal;
	    this.p = p;
	    this.surfaceVoxelsToNormalMap = surfaceVoxelsToNormalMap;
	    this.offset = offset;

	    float[] u = new float[3];
	    float[] v = new float[3];
	    // step 3: calculate tangent vectors, u and v
	    this.calculateTangentVectors(u, v);

	    List<List<Integer>> neighboringSurfaceVoxels = getNeighboringSurfaceVoxels();
	    this.u = new FundamentalForm_x(normal, p, u, neighboringSurfaceVoxels, surfaceVoxelsToNormalMap);
	    this.v = new FundamentalForm_x(normal, p, v, neighboringSurfaceVoxels, surfaceVoxelsToNormalMap);

	}

	private void calculateTangentVectors(float[] u, float[] v) {
	    float[] sortedNormal = new float[] { Math.abs(this.normal[0]), Math.abs(this.normal[1]),
		    Math.abs(this.normal[2]) };
	    Arrays.sort(sortedNormal);

	    ArrayList<float[]> bs = new ArrayList<float[]>();
	    bs.add(new float[] { 1, 0, 0 });
	    bs.add(new float[] { 0, 1, 0 });
	    bs.add(new float[] { 0, 0, 1 });

	    int b_alpha_i = -1, b_beta_i = -1;

	    // SORT BY ABSOLUTE VALUE I ASSUME?
	    for (int i = 0; i < 3; i++) {
		if (Math.abs(this.normal[i]) == sortedNormal[0] && b_alpha_i == -1) {
		    b_alpha_i = i;
		} else if (Math.abs(this.normal[i]) == sortedNormal[1] && b_beta_i == -1) {
		    b_beta_i = i;
		}
	    }

	    float[] b_alpha = bs.get(b_alpha_i);
	    float[] b_beta = bs.get(b_beta_i);
	    for (int i = 0; i < 3; i++) {
		u[i] = b_alpha[i] - this.normal[b_alpha_i] * this.normal[i];
		v[i] = b_beta[i] - this.normal[b_beta_i] * this.normal[i];
	    }

	    normalize(u);
	    normalize(v);
	}

	private List<List<Integer>> getNeighboringSurfaceVoxels() {
	    List<List<Integer>> neighboringSurfaceVoxels = new ArrayList<List<Integer>>();
	    for (int dx = -1; dx <= 1; dx++) {
		for (int dy = -1; dy <= 1; dy++) {
		    for (int dz = -1; dz <= 1; dz++) {
			if (!(dx == 0 && dy == 0 && dz == 0)) {// should we exclude current voxel?
			    List<Integer> possibleNeighbor = Arrays.asList(ArrayUtils.toObject(
				    new int[] { this.p.get(0) + dx, this.p.get(1) + dy, this.p.get(2) + dz }));
			    if (this.surfaceVoxelsToNormalMap.keySet().contains(possibleNeighbor)) {
				neighboringSurfaceVoxels.add(possibleNeighbor);
			    }
			}
		    }
		}
	    }
	    if (neighboringSurfaceVoxels.isEmpty()) {
		System.out.println("why");
	    }
	    return neighboringSurfaceVoxels;
	}

	private void getXs() {
	    this.X_u = this.u.getX_x();
	    this.X_v = this.v.getX_x();
	}

	private void getNs() {
	    this.N_u = this.u.getN_x();
	    this.N_v = this.v.getN_x();
	}

	private void getFirstFundamentalForm() {
	    // http://web.cs.iastate.edu/~cs577/handouts/gaussian-curvature.pdf
	    // Step 4: get X_u and X_v
	    this.getXs();

	    // fundamental forms
	    this.E = scalarProduct(this.X_u, this.X_u);
	    this.F = scalarProduct(this.X_u, this.X_v);
	    this.G = scalarProduct(this.X_v, this.X_v);
	}

	private void getSecondFundamentalForm() {
	    // step 5: normals
	    this.getNs();

	    // second fundamental form
	    this.L = -scalarProduct(this.X_u, this.N_u);
	    this.M1 = -scalarProduct(this.X_u, this.N_v);
	    this.M2 = -scalarProduct(this.X_v, this.N_u); // note that according to paper, it the off diagonal elements
							  // aren't necessarily symmetric
	    this.N = -scalarProduct(this.X_v, this.N_v);
	}

	public void calculateCurvatures() {
	    // step 6: calculate curvatures
	    this.getFirstFundamentalForm();
	    this.getSecondFundamentalForm();

	    // note that according to paper, it the off diagonal elements aren't necessarily
	    // symmetric so have m1 and m2 instead of just m
	    this.gaussianCurvature = (L * N - M1 * M2) / (E * G - F * F);
	    if (Float.isNaN(this.gaussianCurvature)) {
//		System.out.println("why");
	    }
	    this.meanCurvature = (float) (0.5 * (L * G - (M1 + M2) * F + N * E) / (E * G - F * F));
	}

    }

    public static class FundamentalForm_x {
	public float[] N_p = new float[3];
	public List<Integer> p = new ArrayList<Integer>();
	public float[] x;
	public float[] p_x;
	public float[] p_plus_x;
	public List<Integer> nearestNeighborSurfaceVoxel_x;
	public HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap;
	public List<List<Integer>> neighboringSurfaceVoxels;

	FundamentalForm_x(float[] normal, List<Integer> p, float[] x, List<List<Integer>> neighboringSurfaceVoxels,
		HashMap<List<Integer>, float[]> surfaceVoxelsToNormalMap) {
	    this.N_p = normal;
	    this.p = p;
	    this.x = x;
	    this.surfaceVoxelsToNormalMap = surfaceVoxelsToNormalMap;
	    this.neighboringSurfaceVoxels = neighboringSurfaceVoxels;
	}

	private float[] getX_x() {
	    getP_x();
	    float[] X_x = { this.p_x[0] - this.p.get(0), this.p_x[1] - this.p.get(1), this.p_x[2] - this.p.get(2) };
	    return X_x;
	}

	private void getP_x() {
	    this.p_plus_x = new float[] { this.p.get(0) + this.x[0], this.p.get(1) + this.x[1],
		    this.p.get(2) + this.x[2] };
	    float minDist = (float) 1E9;
	    float dX, dY, dZ, currentDist;
	    for (List<Integer> neighboringSurfaceVoxel : this.neighboringSurfaceVoxels) {
		dX = neighboringSurfaceVoxel.get(0) - p_plus_x[0];
		dY = neighboringSurfaceVoxel.get(1) - p_plus_x[1];
		dZ = neighboringSurfaceVoxel.get(2) - p_plus_x[2];
		currentDist = dX * dX + dY * dY + dZ * dZ;
		if (currentDist < minDist) {
		    minDist = currentDist;
		    this.nearestNeighborSurfaceVoxel_x = neighboringSurfaceVoxel;
		}
	    }

	    float[] normal = this.surfaceVoxelsToNormalMap.get(this.nearestNeighborSurfaceVoxel_x);
	    this.p_x = projectPointOntoPlane(this.p_plus_x, this.nearestNeighborSurfaceVoxel_x, normal);
	}

	private float[] projectPointOntoPlane(float[] p, List<Integer> o, float[] n) {
	    // https://stackoverflow.com/questions/9605556/how-to-project-a-point-onto-a-plane-in-3d
	    float[] v = { p[0] - o.get(0), p[1] - o.get(1), p[2] - o.get(2) };

	    float d = scalarProduct(v, n);
	    float[] projection = { p[0] - d * n[0], p[1] - d * n[1], p[2] - d * n[2] };

	    return projection;
	}

	private float[] getN_x() {
	    float[] N_p_plus_x = weightedAverageOfNormals_p_plus_x();// not doing linear interpolation...
	    float[] partialDerivative = { this.N_p[0] - N_p_plus_x[0], this.N_p[1] - N_p_plus_x[1],
		    this.N_p[2] - N_p_plus_x[2] };

	    float[] normal = this.surfaceVoxelsToNormalMap.get(this.nearestNeighborSurfaceVoxel_x);
	    float[] N_x_start = projectPointOntoPlane(new float[] { 0, 0, 0 }, nearestNeighborSurfaceVoxel_x, normal);
	    float[] N_x_end = projectPointOntoPlane(partialDerivative, nearestNeighborSurfaceVoxel_x, normal);

	    float[] N_x = new float[3];
	    for (int d = 0; d < 3; d++) {
		N_x[d] = N_x_end[d] - N_x_start[d];
		// System.out.println(N_x[d]);
	    }
	    if (Float.isNaN(N_x[0])) {
		System.out.println();
	    }

	    return N_x;
	}

	private static float interpolate1D(float v1, float v2, float x) {
	    return v1 * (1 - x) + v2 * x;
	}

	private static float interpolate2D(float v1, float v2, float v3, float v4, float x, float y) {

	    float s = interpolate1D(v1, v2, x);
	    float t = interpolate1D(v3, v4, x);
	    return interpolate1D(s, t, y);
	}

	private static float interpolate3D(float v1, float v2, float v3, float v4, float v5, float v6, float v7,
		float v8, float x, float y, float z) { // wiki and
						       // https://stackoverflow.com/questions/19271568/trilinear-interpolation
						       // need to check
	    float s = interpolate2D(v1, v2, v3, v4, x, y);
	    float t = interpolate2D(v5, v6, v7, v8, x, y);
	    return interpolate1D(s, t, z);
	}

	private float[] weightedAverageOfNormals_p_plus_x() {
	    // p+x seems dumb since it won't be on surface...so we'll use the voxels
	    // neighboring the voxel we projected onto
	    // int [] containingVoxel = new int[] {(int) Math.floor(this.p_plus_x[0]+0.5),
	    // (int) Math.floor(this.p_plus_x[1]+0.5), (int)
	    // Math.floor(this.p_plus_x[2]+0.5)};
	    float[] averageNormal = { 0, 0, 0 };
	    float[] sum_dist = { 0, 0, 0 };
	    for (int dx = -1; dx <= 1; dx++) {
		for (int dy = -1; dy <= 1; dy++) {
		    for (int dz = -1; dz <= 1; dz++) {
			List<Integer> possibleSurfaceVoxel = Arrays.asList(nearestNeighborSurfaceVoxel_x.get(0) + dx,
				nearestNeighborSurfaceVoxel_x.get(1) + dy, nearestNeighborSurfaceVoxel_x.get(2) + dz);
			if (this.surfaceVoxelsToNormalMap.containsKey(possibleSurfaceVoxel)) {
			    float[] neighboringNormal = this.surfaceVoxelsToNormalMap.get(possibleSurfaceVoxel);
			    float[] separation = { this.p_plus_x[0] - possibleSurfaceVoxel.get(0),
				    this.p_plus_x[1] - possibleSurfaceVoxel.get(1),
				    this.p_plus_x[2] - possibleSurfaceVoxel.get(2) };
			    float dist = (float) Math.sqrt(Math.pow(separation[0], 2) + Math.pow(separation[1], 2)
				    + Math.pow(separation[2], 2));
			    if (dist == 0) {
				return neighboringNormal;
			    } else {
				for (int d = 0; d < 3; d++) {
				    averageNormal[d] += neighboringNormal[d] / dist;
				    sum_dist[d] += 1 / dist;
				}
			    }
			}
		    }
		}
	    }
	    for (int d = 0; d < 3; d++) {
		averageNormal[d] /= sum_dist[d];
	    }

	    normalize(averageNormal);
	    return averageNormal;
	}
    }

    public static float scalarProduct(float[] v1, float[] v2) {
	float sp = 0;
	for (int d = 0; d < 3; d++) {
	    sp += v1[d] * v2[d];
	}
	return sp;
    }

    private static <T extends IntegerType<T> & NativeType<T>> RandomAccessibleInterval<UnsignedLongType> getInternalGraidentUsingCube(
	    RandomAccessibleInterval<T> source, int halfLength) {
	long[] dimensions = new long[3];
	long[] pos;

	source.dimensions(dimensions);
	RandomAccess<T> sourceRA = source.randomAccess();
	RandomAccessibleInterval<UnsignedLongType> mask = ArrayImgs.unsignedLongs(dimensions);

	RandomAccess<UnsignedLongType> maskRA = mask.randomAccess();
	for (long x = halfLength; x < dimensions[0] - halfLength; x++) {
	    for (long y = halfLength; y < dimensions[1] - halfLength; y++) {
		for (long z = halfLength; z < dimensions[2] - halfLength; z++) {
		    pos = new long[] { x, y, z };
		    long referenceID = isInInternalGradient(sourceRA, pos, halfLength);
		    if (referenceID > 0) {
			maskRA.setPosition(pos);
			maskRA.get().set(referenceID);
		    }
		}
	    }
	}

	return mask;
    }

    private static <T extends IntegerType<T> & NativeType<T>> HashMap<List<Integer>, float[]> calculateNormalsAtSurfaceVoxels(
	    RandomAccessibleInterval<T> source, RandomAccessibleInterval<UnsignedLongType> mask,
	    int windowSizeParameter) {
	RandomAccess<T> sourceRA = source.randomAccess();
	long[] dimensions = new long[3];
	int[] pos;
	// List<Integer> posList;

	source.dimensions(dimensions);
	// neighbors: ‖(x,y,z)−(x′,y′,z′)‖∞≤1 from paper
	// TODO: if multiple objects in image
	// normals map:
	HashMap<List<Integer>, float[]> surfaceVoxelToNormalMap = new HashMap<List<Integer>, float[]>();

	for (int x = windowSizeParameter; x < dimensions[0] - windowSizeParameter; x++) {
	    for (int y = windowSizeParameter; y < dimensions[1] - windowSizeParameter; y++) {
		for (int z = windowSizeParameter; z < dimensions[2] - windowSizeParameter; z++) {
		    pos = new int[] { x, y, z };
		    sourceRA.setPosition(pos);
		    long referenceID = sourceRA.get().getIntegerLong();
		    if (referenceID > 0) {
			// Check neighborhood
			checkNeighborhood(pos, referenceID, sourceRA, mask, windowSizeParameter,
				surfaceVoxelToNormalMap);

		    }
		}
	    }
	}
	return surfaceVoxelToNormalMap;
    }

    private static <T extends IntegerType<T> & NativeType<T>> void checkNeighborhood(int[] pos, long referenceID,
	    RandomAccess<T> sourceRA, RandomAccessibleInterval<UnsignedLongType> mask, int windowSizeParameter,
	    HashMap<List<Integer>, float[]> surfaceVoxelToNormalMap) {
	int[] neighborPos;
	for (int dx = -1; dx <= 1; dx++) {
	    for (int dy = -1; dy <= 1; dy++) {
		for (int dz = -1; dz <= 1; dz++) {
		    if (!(dx == 0 && dy == 0 && dz == 0)) {
			neighborPos = new int[] { pos[0] + dx, pos[1] + dy, pos[2] + dz };
			sourceRA.setPosition(neighborPos);
			if (sourceRA.get().getIntegerLong() != referenceID) { // then pos is surface voxel
			    float[] normal = calculateNormal(mask, pos, referenceID, windowSizeParameter);
			    // posList = Arrays.stream(pos).boxed().collect(Collectors.toList());
			    surfaceVoxelToNormalMap.put(Arrays.asList(ArrayUtils.toObject(pos)), normal);
			    return;
			}
		    }
		}
	    }
	}
    }

    private static <T extends IntegerType<T> & NativeType<T>> float[] calculateNormal(RandomAccessibleInterval<T> mask,
	    int[] pos, long referenceID, int windowSizeParameter) {
	int com[] = { 0, 0, 0 };
	float count = 0;
	RandomAccess<T> maskRA = mask.randomAccess();
	for (int dx = -windowSizeParameter; dx <= windowSizeParameter; dx++) {
	    for (int dy = -windowSizeParameter; dy <= windowSizeParameter; dy++) {
		for (int dz = -windowSizeParameter; dz <= windowSizeParameter; dz++) {
		    maskRA.setPosition(new int[] { pos[0] + dx, pos[1] + dy, pos[2] + dz });
		    if (maskRA.get().getIntegerLong() == referenceID) {
			com[0] += dx;
			com[1] += dy;
			com[2] += dz;
			count++;
		    }
		}
	    }
	}
	float[] normal = new float[] { com[0] / count, com[1] / count, com[2] / count };
	// normalized:
	normalize(normal);
	return normal;
    }

    private static void normalize(float[] v) {
	double vMag = Math.sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2]);
	for (int i = 0; i < 3; i++) {
	    v[i] /= vMag;
	}
    }

    private static <T extends IntegerType<T> & NativeType<T>> long isInInternalGradient(RandomAccess<T> sourceRA,
	    long[] pos, int halfLength) {
	sourceRA.setPosition(pos);
	long[] newPos;
	if (sourceRA.get().getIntegerLong() == 0) {
	    return 0;
	} else {
	    long referenceID = sourceRA.get().getIntegerLong();
	    for (long dx = -halfLength; dx <= halfLength; dx++) {
		for (long dy = -halfLength; dy <= halfLength; dy++) {
		    for (long dz = -halfLength; dz <= halfLength; dz++) {
			newPos = new long[] { pos[0] + dx, pos[1] + dy, pos[2] + dz };
			sourceRA.setPosition(newPos);
			if (sourceRA.get().getIntegerLong() == 0) {
			    if (pos[0] == 81 && pos[1] == 105 && pos[2] > 40) {
				// System.out.println("hey");
			    }
			    return referenceID;
			}
		    }
		}
	    }
	}
	return 0;
    }

    private static void setupSparkAndCalculateGaussianAndMeanCurvatures(String inputN5Path, String inputN5DatasetName,
	    String outputN5Path) throws IOException {
	final SparkConf conf = new SparkConf().setAppName("SparkGaussianAndMeanCurvatures");

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
	    calculateGaussianAndMeanCurvature(sc, inputN5Path, currentOrganelle, outputN5Path, finalOutputN5DatasetName,
		    blockInformationList);

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
//based on paper https://reader.elsevier.com/reader/sd/pii/S1524070315000284?token=E05DA4F8B7DCB14C483033605A07220AE06753F962D87D3C5186D4A3B6950CCA999324F9849D683CD79735CB78DA39FD
	final Options options = new Options(args);

	if (!options.parsedSuccessfully)
	    return;

	String inputN5DatasetName = options.getInputN5DatasetName();
	String inputN5Path = options.getInputN5Path();
	String outputN5Path = options.getOutputN5Path();

	setupSparkAndCalculateGaussianAndMeanCurvatures(inputN5Path, inputN5DatasetName, outputN5Path);

    }

}
