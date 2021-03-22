package org.janelia.cosem.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.Scale2D;
import net.imglib2.realtransform.Scale3D;
import net.imglib2.transform.integer.MixedTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.MixedTransformView;
import net.imglib2.view.Views;

//Modified based on https://github.com/saalfeldlab/template-building/blob/n5spark/src/main/java/io/IOHelper.java
public class IOHelper {

	ImagePlus ip;
	RandomAccessibleInterval<?> rai;
	double[] resolution;

	public static ResolutionGet[] resolutionGetters = new ResolutionGet[] { new Resolution(), new PixelResolution(),
			new ElemSizeUmResolution(), new TransformResolution() };

	final Logger logger = LoggerFactory.getLogger(IOHelper.class);

	public static void main(String[] args) {
	}

//	public void setResolutionAttribute( final String resolutionAttribute )
//	{
//		this.resolutionAttribute = resolutionAttribute;
//	}
//
//	public void setOffsetAttribute( final String offsetAttribute )
//	{
//		this.offsetAttribute = offsetAttribute;
//	}

	public double[] getResolution() {
		return resolution;
	}

	public static double[] getResolution(final N5Reader n5, final String dataset) {
		Set<String> attrKeys;
		try {
			attrKeys = n5.listAttributes(dataset).keySet();
			double[] resolution = null;
			for (ResolutionGet rg : resolutionGetters) {
				if (attrKeys.contains(rg.getKey())) {
					ResolutionGet rgInstance;
					if (rg.isSimple())
						rgInstance = rg.create(n5.getAttribute(dataset, rg.getKey(), double[].class));
					else
						rgInstance = (ResolutionGet) n5.getAttribute(dataset, rg.getKey(), rg.getClass());

					resolution = rgInstance.getResolution();
					return resolution;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new double[] { 4, 4, 4 };// Default to 4 nm
	}

	public static int[] getOffset(final N5Reader n5, final String dataset) throws IOException {
		int[] offset = n5.getAttribute(dataset, "offset", int[].class);
		if (offset == null) {
			offset = new int[] { 0, 0, 0 };
		}
		return offset;
	}

	@SuppressWarnings("unchecked")
	public <T extends RealType<T> & NativeType<T>> T getType() {
		if (rai != null)
			return (T) Util.getTypeFromInterval(rai);

		else if (ip != null)
			if (ip.getBitDepth() == 8) {
				return (T) new UnsignedByteType();
			} else if (ip.getBitDepth() == 16) {
				return (T) new UnsignedShortType();
			} else if (ip.getBitDepth() == 32) {
				return (T) new FloatType();
			}
		return null;
	}

	public ImagePlus getIp() {
		return ip;
	}

	@SuppressWarnings("unchecked")
	public <T extends RealType<T> & NativeType<T>> RandomAccessibleInterval<T> getRai() {
		return (RandomAccessibleInterval<T>) rai;
	}

	// public <T extends RealType<T> & NativeType<T>> ImagePlus toImagePlus(
	// RandomAccessibleInterval<T> img )

	/**
	 * Permutes the dimensions of a {@link RandomAccessibleInterval} using the given
	 * permutation vector, where the ith value in p gives destination of the ith
	 * input dimension in the output.
	 *
	 * @param source the source data
	 * @param p      the permutation
	 * @return the permuted source
	 */
	public static final <T> IntervalView<T> permute(RandomAccessibleInterval<T> source, int[] p) {
		final int n = source.numDimensions();

		final long[] min = new long[n];
		final long[] max = new long[n];
		for (int i = 0; i < n; ++i) {
			min[p[i]] = source.min(i);
			max[p[i]] = source.max(i);
		}

		final MixedTransform t = new MixedTransform(n, n);
		t.setComponentMapping(p);

		return Views.interval(new MixedTransformView<T>(source, t), min, max);
	}

	/**
	 * Permutes the dimensions of a {@link RandomAccessibleInterval} using the given
	 * permutation vector, where the ith value in p gives destination of the ith
	 * input dimension in the output.
	 *
	 * @param source the source data
	 * @param p      the permutation
	 * @return the permuted source
	 */
	public static final <T> IntervalView<T> reverseDims(RandomAccessibleInterval<T> source, double[] res) {
		assert source.numDimensions() == res.length;

		final int n = source.numDimensions();
		double[] tmp = new double[n];

		final int[] p = new int[n];
		for (int i = 0; i < n; ++i) {
			p[i] = n - 1 - i;
			tmp[i] = res[n - i - 1];
		}
		System.arraycopy(tmp, 0, res, 0, n);

		return permute(source, p);
	}

	public static AffineGet pixelToPhysicalN5(final N5Reader n5, final String dataset) {
		try {
			if (n5.datasetExists(dataset)) {
				long[] size = (long[]) n5.getAttribute(dataset, "dimensions", long[].class);
				int nd = size.length;

//				double[] resolutions = (double[])n5.getAttribute( dataset, resolutionAttribute, double[].class );
//				if( resolutions == null )
//				{
//					resolutions = new double[ size.length ];
//					Arrays.fill( resolutions, 1.0 );
//				}

				Set<String> attrKeys = n5.listAttributes(dataset).keySet();
				double[] resolution = null;
				for (ResolutionGet rg : resolutionGetters) {
					if (attrKeys.contains(rg.getKey())) {
						ResolutionGet rgInstance = (ResolutionGet) n5.getAttribute(dataset, rg.getKey(), rg.getClass());
						resolution = rgInstance.getResolution();
						break;
					}
				}

				if (resolution == null) {
					resolution = new double[size.length];
					Arrays.fill(resolution, 1.0);
				}

				// TODO implement
//				double[] offset = (double[])n5.getAttribute( dataset, offsetAttribute, double[].class );
				double[] offset = null;

				if (nd == 1) {
					AffineTransform affine = new AffineTransform(1);
					affine.set(resolution[0], 0, 0);
					// affine.set( offset[ 0 ], 0, 1 ); // TODO
					return affine;
				} else if (nd == 2 && offset == null) {
					return new Scale2D(resolution);
				} else if (nd == 3 && offset == null) {
					return new Scale3D(resolution);
				}
//				else if ( nd == 2 && offset != null )
//				{
//					AffineTransform2D affine = new AffineTransform2D();
//				}
////				else if ( nd == 3 && offset != null )
//				{
//					AffineTransform3D affine = new AffineTransform3D();
//				}

				return null;

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static interface ResolutionGet {
		public String getKey();

		public double[] getResolution();

		public boolean isSimple();

		public ResolutionGet create(double[] in);
	}

	public static class PixelResolution implements ResolutionGet {
		public static final String key = "pixelResolution";
		public double[] dimensions;
		public String unit = "nm"; // Hardcode for now

		public PixelResolution() {
		};

		public PixelResolution(double[] in) {
			dimensions = in;
		};

		public double[] getResolution() {
			return dimensions;
		}

		public String getKey() {
			return key;
		};

		public boolean isSimple() {
			return false;
		}

		public ResolutionGet create(double[] in) {
			return null;
		}
	}

	public static class Resolution implements ResolutionGet {
		public static final String key = "resolution";
		public static final boolean simple = true;

		public Resolution() {
		}

		public Resolution(double[] in) {
			resolution = in;
		}

		public double[] resolution;

		public double[] getResolution() {
			return resolution;
		}

		public String getKey() {
			return key;
		};

		public boolean isSimple() {
			return true;
		}

		public ResolutionGet create(double[] in) {
			return new Resolution(in);
		}
	}

	public static class ElemSizeUmResolution implements ResolutionGet {
		public static final String key = "element_size_um";
		public static final boolean simple = true;

		public ElemSizeUmResolution() {
		}

		public ElemSizeUmResolution(double[] in) {
			element_size_um = in;
		}

		public double[] element_size_um;

		public double[] getResolution() {
			return element_size_um;
		}

		public String getKey() {
			return key;
		};

		public boolean isSimple() {
			return true;
		}

		public ResolutionGet create(double[] in) {
			return new ElemSizeUmResolution(in);
		}
	}

	public static class TransformResolution implements ResolutionGet {
		public static final String key = "transform";
		public static final boolean simple = false;
		public double[] scale;
		public double[] translation;
		public String[] units;

		public TransformResolution() {
		}

		public TransformResolution(double[] s, double[] t, String[] u) {
			scale = s;
			translation = t;
			units = u;
		}

		public double[] getResolution() {
			return scale;
		}

		public String getKey() {
			return key;
		};

		public boolean isSimple() {
			return simple;
		}

		public ResolutionGet create(double[] in) {
			String[] units = new String[in.length];
			Arrays.fill(units, "pixel");
			return new TransformResolution(in, new double[in.length], units);
		}
	}

}