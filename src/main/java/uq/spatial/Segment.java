package uq.spatial;

import java.io.Serializable;

/**
 * A trajectory segment object with time stamp.
 * Line segment from coordinate points (x1,y1,t1) to (x2,y2,t2).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Segment implements Serializable {
	// segment id
	public int id;
	// segment end point coordinates
	public double x1;
	public double y1;
	public double x2;
	public double y2;
	// end points time-stamp
	public long t1;
	public long t2;
	
	public Segment(){}
	public Segment(double x1, double y1, long t1, double x2, double y2, long t2) {
		this.x1 = x1;
		this.x2 = x2;
		this.y1 = y1;
		this.y2 = y2;
		this.t1 = t1;
		this.t2 = t2;
	}
	public Segment(double x1, double y1, double x2, double y2) {
		this.x1 = x1;
		this.x2 = x2;
		this.y1 = y1;
		this.y2 = y2;
	}
	public Segment(Point p1, Point p2) {
		this.x1 = p1.x;
		this.y1 = p1.y;
		this.x2 = p2.x;
		this.y2 = p2.y;
		this.t1 = p1.time;
		this.t2 = p2.time;
	}
}
