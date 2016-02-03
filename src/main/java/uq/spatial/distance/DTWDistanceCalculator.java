package uq.spatial.distance;

import java.io.Serializable;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * DTW: Dynamic Time Warping for time series.
 * 
 * </br> Uniform sampling rates only.
 * </br> Spatial dimension only.
 * </br> Cope with local time shift.
 * </br> Sensitive to noise.
 * </br> Not a metric.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class DTWDistanceCalculator implements Serializable, TrajectoryDistanceCalculator{

	private double [][]costMatrix;

	public double getDistance(Trajectory t1, Trajectory t2){
		return DTW(t1.getPointsList(), t2.clone().getPointsList());
	}
	
	/**
	 * DTW dynamic.
	 */
	private double DTW(List<Point> t1, List<Point> t2){
		// current iteration
		int size_t1 = t1.size();
		int size_t2 = t2.size();

		// initialize dynamic matrix
		costMatrix = new double[size_t1+1][size_t2+1];
		costMatrix[0][0] = 0.0;
		for(int i=1; i<=size_t1; i++){
			costMatrix[i][0] = INFINITY;
		}
		for(int i=1; i<=size_t2; i++){
			costMatrix[0][i] = INFINITY;
		}
		
		// dynamic DTW calculation
		double dist, cost_a, cost_b, cost_c;
		for(int i=1; i<=size_t1; i++){
			for(int j=1; j<=size_t2; j++){
				dist = t1.get(i-1).dist(t2.get(j-1));
				cost_a = costMatrix[i-1][j-1];
				cost_b = costMatrix[i-1][j];
				cost_c = costMatrix[i][j-1];				
				costMatrix[i][j] = dist + min(cost_a, cost_b, cost_c);
			}
		}
		
		return costMatrix[size_t1][size_t2];
	}
	
	/**
	 * The minimum between a, b and c.
	 */
	private double min(double a, double b, double c){
		if (a <= b && a <= c){
			return a;
		}
		if (b <= c){
			return b;
		}
		return c;
	}

}
