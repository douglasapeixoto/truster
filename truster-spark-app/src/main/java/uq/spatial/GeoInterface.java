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
	// small number
	static final double SMALL_NUM = 0.0001;
	
	// grid/space dimensions		   //# Dataset					//# Query 	
	final static double MIN_X = 375.0; // MinX: 52.99205499607079   // Min X: 375.7452259303738
	final static double MIN_Y =  16.0; // MinY: -20.08557496216634  // Min Y: 16.319751123918174
	final static double MAX_X = 577.0; // MaxX: 716.4193496072005   // Max X: 576.9230902330686
	final static double MAX_Y = 235.0; // MaxY: 395.5344310979076   // Max Y: 234.80924053063617

	// Earth radius (average) in meters
	static final int EARTH_RADIUS = 6371000;
	
	// pi
	static final double PI = Math.PI;
}