package uq.spatial;

import java.io.Serializable;

import uq.spatial.distance.EuclideanDistanceCalculator;
import uq.spatial.distance.PointDistanceCalculator;

/**
 * A 2D line segment object.
 * </br>
 * Line segment from coordinate points (x1,y1) to (x2,y2).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Segment implements Serializable {
	public double x1;
	public double y1;
	public double x2;
	public double y2;

	public Segment(){}
	public Segment(double x1, double y1, double x2, double y2) {
		this.x1 = x1;
		this.x2 = x2;
		this.y1 = y1;
		this.y2 = y2;
	}

	/**
	 * The length of this line segment.
	 */
	public double lenght(){
		PointDistanceCalculator dist = 
				new EuclideanDistanceCalculator();
		return dist.getDistance(x1, y1, x2, y2);
	}
	
	/**
	 * True is this segment is parallel to the Y axis.
	 */
	public boolean isVertical(){
		return (x2-x1)==0;
	}
	
	/**
	 * True is this segment is parallel to the X axis.
	 */
	public boolean isHorizontal(){
		return (y2-y1)==0;
	}
	
	/**
	 * True if these two line segments intersect.
	 */
	public boolean intersect(Segment s){
		return intersect(s.x1, s.y1, s.x2, s.y2);
	}
	
	/**
	 * True if these two line segments intersect.
	 * Line segments given by end points X and Y coordinates.
	 */
	public boolean intersect(double x1, double y1, double x2, double y2){
		// vectors r and s
		double rx = x2 - x1;
		double ry = y2 - y1;		
		double sx = this.x2 - this.x1;
		double sy = this.y2 - this.y1;
		
		// cross product r x s
		double cross = (rx*sy) - (ry*sx);
			
		// they are parallel or colinear
		if(cross == 0.0) return false;
	
		double t = (this.x1 - x1)*sy - (this.y1 - y1)*sx;
			   t = t / cross;
		double u = (this.x1 - x1)*ry - (this.y1 - y1)*rx;
			   u = u / cross;

	    if(t > 0.0 && t < 1.0 && 
	       u > 0.0 && u < 1.0){
	    	return true;
	    }
	    return false;
	}
}
