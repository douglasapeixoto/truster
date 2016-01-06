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
	public long timeIni=0;
	public long timeEnd=0;
			
	public STRectangle() {}
	public STRectangle(double minX, double maxX, double minY, double maxY, long timeIni, long timeEnd) {
		super(minX, maxX, minY, maxY);
		this.timeIni = timeIni;
		this.timeEnd = timeEnd;
	}

	@Override
	public String toString(){
		String s = minX + " " + maxX + " " + minY + " " + maxY + " " +
				   timeIni + " " + timeEnd;
		return s;
	}
}
