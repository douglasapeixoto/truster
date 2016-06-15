package uq.spatial;

import java.io.Serializable;

/**
 * A Spatial temporal rectangle.
 * Composed by a spatial region (rectangle) 
 * and a time interval.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class STRectangle extends Rectangle implements Serializable {
	public long t1=0;
	public long t2=0;
			
	public STRectangle() {}
	public STRectangle(double min_x, double min_y, double max_x, double max_y, long t1, long t2) {
		super(min_x, min_y, max_x, max_y);
		this.t1 = t1;
		this.t2 = t2;
	}

	@Override
	public String toString(){
		String s = min_x + " " + max_x + " " + min_y + " " + max_y + " " +
				   t1 + " " + t2;
		return s;
	}
}
