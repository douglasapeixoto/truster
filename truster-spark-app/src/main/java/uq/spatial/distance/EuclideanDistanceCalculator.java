package uq.spatial.distance;

import java.io.Serializable;

import uq.spatial.Point;

/**
 * Eculidean Distance between two points in 2D
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class EuclideanDistanceCalculator implements Serializable, PointDistanceCalculator {

	public double getDistance(double x1, double y1, double x2, double y2){
		double dist2 = (x1-x2)*(x1-x2) + 
					   (y1-y2)*(y1-y2);
		return Math.sqrt(dist2);
	}

	public double getDistance(Point p1, Point p2) {
		return getDistance(p1.x, p1.y, p2.x, p2.y);
	}
}
