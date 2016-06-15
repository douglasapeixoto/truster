package uq.spatial.distance;

import uq.spatial.Point;

/**
 * Interface for distance functions between points.
 * 
 * @author uqdalves
 *
 */
public interface PointDistanceCalculator {
	static final double INFINITY = Double.MAX_VALUE;
	
	/**
	 * Distance between two points.
	 */
	double getDistance(Point p1, Point p2);
	
	/**
	 * Distance between two points given by x and y coordinates.
	 */
	double getDistance(double x1, double y1, double x2, double y2);
}
