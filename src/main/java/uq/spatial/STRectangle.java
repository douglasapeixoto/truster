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
	public STRectangle(double minX, double minY, double maxX, double maxY, long t1, long t2) {
		super(minX, minY, maxX, maxY);
		this.t1 = t1;
		this.t2 = t2;
	}

	@Override
	public String toString(){
		String s = minX + " " + maxX + " " + minY + " " + maxY + " " +
				   t1 + " " + t2;
		return s;
	}
}
