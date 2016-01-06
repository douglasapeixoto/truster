package uq.spatial;

/**
 * Setup some geometric variables.
 * 
 * @author uqdalves
 *
 */
public interface GeoInterface {
	// infinity value
	static final double INF = Double.MAX_VALUE;
	
	// perimeter of the maximum area this application covers (map area)
	static final double MIN_X = -1000000;
	static final double MAX_X = 1000000;
	static final double MIN_Y = -1000000;
	static final double MAX_Y = 1000000;
	
	// minimum distance between points
	static final double MIN_DIST = 0.00001;
	
	// Earth radius (average) in meters
	static final int EARTH_RADIUS = 6371000;
	
	// pi
	static final double PI = Math.PI;
}
