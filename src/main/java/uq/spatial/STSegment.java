package uq.spatial;

import java.io.Serializable;

/**
 * A Spatial-Temporal line segment object with time stamp.
 * </br>
 * Line segment from points (x1,y1,t1) to (x2,y2,t2).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class STSegment extends Segment implements Serializable{
	// end points time-stamp
	public long t1;
	public long t2;
	
	public STSegment(){}
	public STSegment(double x1, double y1, long t1, double x2, double y2, long t2) {
		super(x1, y1, x2, y2);
		this.t1 = t1;
		this.t2 = t2;
	}
	public STSegment(Point p1, Point p2) {
		super(p1.x, p1.y, p2.x, p2.y);
		this.t1 = p1.time;
		this.t2 = p2.time;
	}
}
