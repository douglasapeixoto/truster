package uq.spatial.distance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * ERP: Edit distance with Real Penalty.
 * 
 * </br> Discrete 2D (x,y) time series only.  
 * </br> Uniform sampling rates only.
 * </br> Sensitive to noise.
 * </br> Cope with local time shift.
 * </br> Metric function.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class ERPDistance2DCalculator implements Serializable, TrajectoryDistanceCalculator{

	private double g_threshold;
	
	private double [][]costMatrix;
	
	// use the origin as reference point
	private final Point origin = new Point(0.0, 0.0);
	
	/**
	 * Set default g distance threshold = 0.0
	 */
	public ERPDistance2DCalculator(){
		g_threshold = 0.00;
	}
	/**
	 * Set distance matching threshold.
	 */
	public ERPDistance2DCalculator(double threshold){
		g_threshold = threshold;
	}
	
	public double getDistance(Trajectory t1, Trajectory t2){
		// normalize the coordinates and make sure 
		// the original trajectory will not be changed
		List<Point> t1_dist = normalize(t1.getPointsList());
		List<Point> t2_dist = normalize(t2.getPointsList());

		return ERP(t1_dist, t2_dist);
	}
	
	/**
	 * ERP dynamic.
	 */
	private double ERP(List<Point> t1, List<Point> t2){
		// current iteration
		int size_t1 = t1.size();
		int size_t2 = t2.size();

		// initialize dynamic matrix
		costMatrix = new double[size_t1+1][size_t2+1];
		costMatrix[0][0] = 0.0;
		for(int i=1; i<=size_t1; i++){
			// use the origin (0, 0) as reference
			costMatrix[i][0] = costMatrix[i-1][0] + t1.get(i-1).dist(origin);
		}
		for(int i=1; i<=size_t2; i++){
			// used the origin (0, 0) as reference
			costMatrix[0][i] = costMatrix[0][i-1] + t2.get(i-1).dist(origin);
		}
		
		// dynamic ERP calculation
		double cost_a, cost_b, cost_c;
		for(int i=1; i<=size_t1; i++){
			for(int j=1; j<=size_t2; j++){
				cost_a = costMatrix[i-1][j-1] + cost(t1.get(i-1), t2.get(j-1));
				cost_b = costMatrix[i-1][j]   + cost(t1.get(i-1)); // gap;
				cost_c = costMatrix[i][j-1]   + cost(t2.get(j-1)); // gap
				costMatrix[i][j] = min(cost_a, cost_b, cost_c);
			}
		}
		
		return costMatrix[size_t1][size_t2];
	}
	
	/**
	 * Normalize the coordinate values in this list,
	 * using the mean and the standard deviation.
	 */
	private List<Point> normalize(List<Point> pointsList){
		List<Point> normList = new ArrayList<Point>();
		if(pointsList.size() == 0){
			return normList;
		}
		
		// get mean and std of the coordinates
		double[] mean = getMean(pointsList); 
		double[] std  = getStd(pointsList, mean);		

		// normalize values
		double norm_x, norm_y;
		for(Point p : pointsList){
			norm_x = (p.x - mean[0]) / std[0];
			norm_y = (p.y - mean[1]) / std[1];
			normList.add(new Point(norm_x, norm_y, p.time));
		}
		
		return normList;
	}
	
	/**
	 * The mean of the x[0] and y[1] values in this list.
	 */
	private double[] getMean(List<Point> list){
		int size = list.size();
		double sum_x = 0.0;
		double sum_y = 0.0;
		for(Point p : list){
			sum_x += p.x;
			sum_y += p.y;
		}
		double[] mean = new double[2];
		mean[0] = sum_x / size;
		mean[1] = sum_y / size;
		
		return mean;
	}
	
	/**
	 * The standard deviation of the x[0] and y[1] 
	 * values in this list.
	 */
	private double[] getStd(List<Point> pointsList, double[] mean){
		double size = pointsList.size();
		double dif_sum2_x = 0.0;
		double dif_sum2_y = 0.0;
		for(Point p : pointsList){
			dif_sum2_x += (p.x - mean[0])*(p.x - mean[0]);
			dif_sum2_y += (p.y - mean[1])*(p.y - mean[1]);
		}
		double[] std = new double[2];
		std[0] = Math.sqrt(dif_sum2_x / (size-1));
		std[1] = Math.sqrt(dif_sum2_y / (size-1));	
		
		return std;
	}
	
	/**
	 * Edit cost (distance).
	 */
	private double cost(Point p1, Point p2){
		return p1.dist(p2);
	}
	
	/**
	 * Edit cost - gap (distance).
	 */
	private double cost(Point p){
		return Math.abs(p.dist(origin) - g_threshold);
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
	
	public static void main(String[] arg){
		Trajectory q = new Trajectory("Q");
		q.addPoint(new Point(1, 1));
		q.addPoint(new Point(2, 2));
		q.addPoint(new Point(3, 3));
		q.addPoint(new Point(4, 4));
		q.addPoint(new Point(5, 5));
		q.addPoint(new Point(6, 6));
		q.addPoint(new Point(7, 7));
		q.addPoint(new Point(8, 8));
		q.addPoint(new Point(9, 9));
		q.addPoint(new Point(10, 10));
		q.addPoint(new Point(11, 9));
		q.addPoint(new Point(12, 8));
		q.addPoint(new Point(13, 7));
		q.addPoint(new Point(14, 6));
		q.addPoint(new Point(15, 5));
		q.addPoint(new Point(16, 4));
		q.addPoint(new Point(17, 3));
		q.addPoint(new Point(18, 2));
		
		Trajectory t1 = new Trajectory("T1");
		t1.addPoint(new Point(1, 1));
		t1.addPoint(new Point(2, 2));
		t1.addPoint(new Point(3, 3));
		t1.addPoint(new Point(4, 5));
		t1.addPoint(new Point(5, 6));
		t1.addPoint(new Point(6, 7));
		t1.addPoint(new Point(7, 8));
		t1.addPoint(new Point(8, 9));
		t1.addPoint(new Point(9, 10));
		t1.addPoint(new Point(10, 9));
		t1.addPoint(new Point(11, 8));
		t1.addPoint(new Point(12, 7));
		t1.addPoint(new Point(13, 6));
		t1.addPoint(new Point(14, 5));
		t1.addPoint(new Point(15, 4));
		t1.addPoint(new Point(16, 3));
		t1.addPoint(new Point(17, 2));
		t1.addPoint(new Point(18, 1));
		
		Trajectory t2 = new Trajectory("T2");
		t2.addPoint(new Point(1, 1));
		t2.addPoint(new Point(2, 2));
		t2.addPoint(new Point(3, 3));
		t2.addPoint(new Point(4, 15));
		t2.addPoint(new Point(5, 6));
		t2.addPoint(new Point(6, 7));
		t2.addPoint(new Point(7, 8));
		t2.addPoint(new Point(8, 9));
		t2.addPoint(new Point(9, 10));
		t2.addPoint(new Point(10, 9));
		t2.addPoint(new Point(11, 8));
		t2.addPoint(new Point(12, 7));
		t2.addPoint(new Point(13, 6));
		t2.addPoint(new Point(14, 5));
		t2.addPoint(new Point(15, 4));
		t2.addPoint(new Point(16, 3));
		t2.addPoint(new Point(17, 2));
		t2.addPoint(new Point(18, 1));
		
		Trajectory t3 = new Trajectory("T3");
		t3.addPoint(new Point(1, 1));
		t3.addPoint(new Point(2, 2));
		t3.addPoint(new Point(3, 3));
		t3.addPoint(new Point(4, 4));
		t3.addPoint(new Point(5, 5));
		t3.addPoint(new Point(6, 6));
		t3.addPoint(new Point(7, 7));
		t3.addPoint(new Point(8, 8));
		t3.addPoint(new Point(9, 9));
		t3.addPoint(new Point(10, -5));
		t3.addPoint(new Point(11, 9));
		t3.addPoint(new Point(12, 8));
		t3.addPoint(new Point(13, 7));
		t3.addPoint(new Point(14, 6));
		t3.addPoint(new Point(15, 5));
		t3.addPoint(new Point(16, 4));
		t3.addPoint(new Point(17, 3));
		t3.addPoint(new Point(18, 2));
		
		Trajectory t4 = new Trajectory("T4");
		t4.addPoint(new Point(1, 1));
		t4.addPoint(new Point(2, 2));
		t4.addPoint(new Point(3, 3));
		t4.addPoint(new Point(4, 4));
		t4.addPoint(new Point(5, 5));
		t4.addPoint(new Point(6, 7));
		t4.addPoint(new Point(7, 8));
		t4.addPoint(new Point(8, 9));
		t4.addPoint(new Point(9, 10));
		t4.addPoint(new Point(10, 9));
		t4.addPoint(new Point(11, 8));
		t4.addPoint(new Point(12, 4));
		t4.addPoint(new Point(13, 5));
		t4.addPoint(new Point(14, 4));
		t4.addPoint(new Point(15, 3));
		t4.addPoint(new Point(16, 2));
		t4.addPoint(new Point(17, 1));
		t4.addPoint(new Point(18, 0));
				
		Trajectory t5 = new Trajectory("T5");
		t5.addPoint(new Point(1, 1));
		t5.addPoint(new Point(2, 2));
		t5.addPoint(new Point(3, 3));
		t5.addPoint(new Point(4, 10));
		t5.addPoint(new Point(5, 11));
		t5.addPoint(new Point(6, 10));
		t5.addPoint(new Point(7, 11));
		t5.addPoint(new Point(8, 10));
		t5.addPoint(new Point(9, 9));
		t5.addPoint(new Point(10, 10));
		t5.addPoint(new Point(11, 9));
		t5.addPoint(new Point(12, 8));
		t5.addPoint(new Point(13, 7));
		t5.addPoint(new Point(14, 6));
		t5.addPoint(new Point(15, 5));
		t5.addPoint(new Point(16, 4));
		t5.addPoint(new Point(17, 3));
		t5.addPoint(new Point(18, 2));
		
		ERPDistance2DCalculator dp = new ERPDistance2DCalculator();
		System.out.println("Q-T1: " + dp.getDistance(q, t1));
		System.out.println("Q-T2: " + dp.getDistance(q, t2));
		System.out.println("Q-T3: " + dp.getDistance(q, t3));
		System.out.println("Q-T4: " + dp.getDistance(q, t4));
		System.out.println("Q-T5: " + dp.getDistance(q, t5));
		
		/*ERPDistance1DCalculator dp = new ERPDistance1DCalculator();
		System.out.println("Q-T1: " + dp.getDistance(convert(q), convert(t1)));
		System.out.println("Q-T2: " + dp.getDistance(convert(q), convert(t2)));
		System.out.println("Q-T3: " + dp.getDistance(convert(q), convert(t3)));
		System.out.println("Q-T4: " + dp.getDistance(convert(q), convert(t4)));
		System.out.println("Q-T5: " + dp.getDistance(convert(q), convert(t5)));*/
	}
	
	public static List<Double> convert(Trajectory t1){
		List<Double> list  = new ArrayList<Double>();
		for(Point p : t1.getPointsList()){
			list.add(p.y);
			System.out.println(p.y);
		}
		System.out.println();
		return list;
	}
}
