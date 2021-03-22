package org.janelia.cosem.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for calculating 3D line in voxelspace using Bressenham3D
 *
 */
public class Bressenham3D {

	/**
	 * Get 3D line in voxel space from start voxel to end voxel
	 * 
	 * @param start Starting voxel
	 * @param end   End voxel
	 * @return
	 */
	public static List<long[]> getLine(long[] start, long[] end) {
		// https://www.geeksforgeeks.org/bresenhams-algorithm-for-3-d-line-drawing/
		// Get 3d line betweeen two points in voxel grid
		long x1 = start[0];
		long y1 = start[1];
		long z1 = start[2];
		long x2 = end[0];
		long y2 = end[1];
		long z2 = end[2];

		ArrayList<long[]> listOfPoints = new ArrayList<long[]>();
		listOfPoints.add(start);
		long dx = Math.abs(x2 - x1);
		long dy = Math.abs(y2 - y1);
		long dz = Math.abs(z2 - z1);

		long xs, ys, zs;
		if (x2 > x1)
			xs = 1;
		else
			xs = -1;
		if (y2 > y1)
			ys = 1;
		else
			ys = -1;
		if (z2 > z1)
			zs = 1;
		else
			zs = -1;

		// # Driving axis is X-axis"
		if (dx >= dy && dx >= dz) {
			long p1 = 2 * dy - dx;
			long p2 = 2 * dz - dx;
			while (x1 != x2) {
				x1 += xs;
				if (p1 >= 0) {
					y1 += ys;
					p1 -= 2 * dx;
				}
				if (p2 >= 0) {
					z1 += zs;
					p2 -= 2 * dx;
				}
				p1 += 2 * dy;
				p2 += 2 * dz;
				listOfPoints.add(new long[] { x1, y1, z1 });
			}
		}

		// # Driving axis is Y-axis"
		else if (dy >= dx && dy >= dz) {
			long p1 = 2 * dx - dy;
			long p2 = 2 * dz - dy;
			while (y1 != y2) {
				y1 += ys;
				if (p1 >= 0) {
					x1 += xs;
					p1 -= 2 * dy;
				}
				if (p2 >= 0) {
					z1 += zs;
					p2 -= 2 * dy;
				}
				p1 += 2 * dx;
				p2 += 2 * dz;
				listOfPoints.add(new long[] { x1, y1, z1 });
			}
		}
		// # Driving axis is Z-axis"
		else {
			long p1 = 2 * dy - dz;
			long p2 = 2 * dx - dz;
			while (z1 != z2) {
				z1 += zs;
				if (p1 >= 0) {
					y1 += ys;
					p1 -= 2 * dz;
				}
				if (p2 >= 0) {
					x1 += xs;
					p2 -= 2 * dz;
				}
				p1 += 2 * dy;
				p2 += 2 * dx;
				listOfPoints.add(new long[] { x1, y1, z1 });
			}
		}
		return listOfPoints;
	}
}
