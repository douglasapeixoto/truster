package uq.spatial;

import java.io.Serializable;

/**
 * A line segment object.
 * </br>
 * Line segment from coordinate points (x1,y1) to (x2,y2).
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

	public Segment(){}
	public Segment(double x1, double y1, double x2, double y2) {
		this.x1 = x1;
		this.x2 = x2;
		this.y1 = y1;
		this.y2 = y2;
	}
}
