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
	
	// grid/space dimensions
	final static double MIN_X = 50.0;  // MinX: 52.99205499607079
	final static double MIN_Y = -25.0; // MinY: -20.08557496216634
	final static double MAX_X = 720.0; // MaxX: 716.4193496072005
	final static double MAX_Y = 400.0; // MaxY: 395.5344310979076
	
	// minimum distance between points
	static final double MIN_DIST = 0.00001;
	
	// Earth radius (average) in meters
	static final int EARTH_RADIUS = 6371000;
	
	// pi
	static final double PI = Math.PI;
}
