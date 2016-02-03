package uq.spatial.distance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * EDR: Edit Distance on Real sequences.
 * 
 * </br> Uniform sampling rates only.
 * </br> Spatial dimension only.
 * </br> Cope with local time shift.
 * </br> Not sensitive to noise.
 * </br> Not a metric.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class EDRDistanceCalculator implements Serializable, TrajectoryDistanceCalculator{
	private double matching_threshold;

	private double [][]costMatrix;
	
	/**
	 * Set default  distance matching threshold = 0.0
	 */
	public EDRDistanceCalculator(){
		matching_threshold = 0.00;
	}
	/**
	 * Set distance matching threshold.
	 */
	public EDRDistanceCalculator(double threshold){
		matching_threshold = threshold;
	}
	
	public double getDistance(Trajectory t1, Trajectory t2) {
		// normalize and make sure the original trajectories will not be changed
		List<Point> t1_norm = normalize(t1.getPointsList());
		List<Point> t2_norm = normalize(t2.getPointsList());
		
		return EDR(t1_norm, t2_norm);
	}

	/**
	 * EDR dynamic.
	 */
	private double EDR(List<Point> t1, List<Point> t2) {
		// current iteration
		int size_t1 = t1.size();
		int size_t2 = t2.size();

		// initialize dynamic matrix
		costMatrix = new double[size_t1+1][size_t2+1];
		costMatrix[0][0] = 0.0;
		for(int i=1; i<=size_t1; i++){
			costMatrix[i][0] = i;
		}
		for(int i=1; i<=size_t2; i++){
			costMatrix[0][i] = i;
		}
		
		// dynamic EDR calculation
		double cost_a, cost_b, cost_c;
		for(int i=1; i<=size_t1; i++){
			for(int j=1; j<=size_t2; j++){
				cost_a = costMatrix[i-1][j-1] + subcost(t1.get(i-1), t2.get(j-1));
				cost_b = costMatrix[i-1][j]   + 1; // gap;
				cost_c = costMatrix[i][j-1]   + 1; // gap
				costMatrix[i][j] = min(cost_a, cost_b, cost_c);
			}
		}
		
		return costMatrix[size_t1][size_t2];
	}

	/**
	 * Distance cost between p1 and p2.
	 */
	private double subcost(Point p1, Point p2) {
		return p1.dist(p2) <= matching_threshold ? 0.0 : 1.0;
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
	
	/**
	 * Normalize the trajectory coordinate points,
	 * using the mean and the standard deviation.
	 *
	 * @return The new normalized list of values.
	 */
	private List<Point> normalize(final List<Point> pointsList){
		List<Point> normList = new ArrayList<Point>();
		if(pointsList.size() == 0){
			return normList;
		}
		
		// get mean and std
		double mean = getMean(pointsList); 
		double std  = getStd(pointsList, mean);		

		// normalize the y values
		double norm_y;
		for(Point p : pointsList){
			norm_y = (p.y - mean) / std;
			normList.add(new Point(p.x, norm_y));
		}
		
		return normList;
	}
	
	/**
	 * The mean of the y coordinates in this points list.
	 */
	private double getMean(List<Point> pointsList){
		int size = pointsList.size();
		double sum = 0;
		for(Point p : pointsList){
			sum += p.y;
		}
		return (sum / size);
	}
	
	/**
	 * The standard deviation of the y coordinates 
	 *  in this points list.
	 */
	private double getStd(List<Point> pointsList, double mean){
		double size = pointsList.size();
		double dif_sum2 = 0;
		for(Point p : pointsList){
			dif_sum2 += (p.y - mean)*(p.y - mean);
		}
		
		return Math.sqrt(dif_sum2 / (size - 1));
	}
}
