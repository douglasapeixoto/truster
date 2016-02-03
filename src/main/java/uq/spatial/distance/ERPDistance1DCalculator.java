package uq.spatial.distance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ERP: Edit distance with Real Penalty.
 * 
 * </br> Discrete 1D time series only.  
 * </br> Uniform sampling rates only.
 * </br> Sensitive to noise.
 * </br> Cope with local time shift.
 * </br> Metric function.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class ERPDistance1DCalculator implements Serializable {
	private double g_threshold;
	
	private double [][]costMatrix;

	/**
	 * Set default g distance threshold = 0.0
	 */
	public ERPDistance1DCalculator(){
		g_threshold = 0.00;
	}
	/**
	 * Set distance matching threshold.
	 */
	public ERPDistance1DCalculator(double threshold){
		g_threshold = threshold;
	}
	
	public double getDistance(List<Double> t1, List<Double> t2){
		// normalize and make sure the original time series will not be changed
		List<Double> t1_norm = normalize(t1);
		List<Double> t2_norm = normalize(t2);
		
		return ERP(t1_norm, t2_norm);
	}
	
	/**
	 * ERP dynamic.
	 */
	private double ERP(List<Double> t1, List<Double> t2){
		// current iteration
		int size_t1 = t1.size();
		int size_t2 = t2.size();

		// initialize dynamic matrix
		costMatrix = new double[size_t1+1][size_t2+1];
		costMatrix[0][0] = 0.0;
		for(int i=1; i<=size_t1; i++){
			costMatrix[i][0] = costMatrix[i-1][0] + t1.get(i-1);
		}
		for(int i=1; i<=size_t2; i++){
			costMatrix[0][i] = costMatrix[0][i-1] + t2.get(i-1);
		}
		
		// dynamic ERP calculation
		double cost_a, cost_b, cost_c;
		for(int i=1; i<=size_t1; i++){
			for(int j=1; j<=size_t2; j++){
				cost_a = costMatrix[i-1][j-1] + cost(t1.get(i-1), t2.get(j-1));
				cost_b = costMatrix[i-1][j]   + cost(t1.get(i-1)); // gap
				cost_c = costMatrix[i][j-1]   + cost(t2.get(j-1)); // gap
				costMatrix[i][j] = min(cost_a, cost_b, cost_c);
			}
		}
		
		return costMatrix[size_t1][size_t2];
	}
	
	/**
	 * Normalize the values in this list
	 * using the mean and the standard deviation.
	 *
	 * @return The new normalized list of values.
	 */
	private List<Double> normalize(List<Double> list){
		List<Double> normList = new ArrayList<Double>();
		if(list.size() == 0){
			return normList;
		}
		
		// get mean and std
		double mean = getMean(list); 
		double std  = getStd(list, mean);		

		// normalize values
		double norm;
		for(Double value : list){
			norm = (value - mean) / std;
			normList.add(norm);
		}
		
		return normList;
	}
	
	/**
	 * The mean of the values in this list.
	 */
	private double getMean(List<Double> list){
		int size = list.size();
		double sum = 0.0;
		for(Double value : list){
			sum += value;
		}
		return (sum / size);
	}
	
	/**
	 * The standard deviation of values in this list.
	 */
	private double getStd(List<Double> list, double mean){
		double size = list.size();
		double dif_sum2 = 0.0;
		for(Double value : list){
			dif_sum2 += (value - mean)*(value - mean);
		}
		return Math.sqrt(dif_sum2 / size);
	}
	
	/**
	 * Edit cost (distance).
	 */
	private double cost(double v1, double v2){
		return Math.abs(v1 - v2);
	}
	
	/**
	 * Edit cost - gap (distance).
	 */
	private double cost(double v){
		return Math.abs(v - g_threshold);
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
	
/*	public static void main(String[] arg){
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
		
		ERPDistanceCalculatorDP dp = new ERPDistanceCalculatorDP();
		
		System.out.println("Q-T1: " + getDistance(q, t1));
		System.out.println("Q-T2: " + getDistance(q, t2));
		System.out.println("Q-T3: " + getDistance(q, t3));
		System.out.println("Q-T4: " + getDistance(q, t4));
		System.out.println("Q-T5: " + getDistance(q, t5));
	}
	
	/**
	 * ERP recursive.
	 */
	/*private static double getERP(List<Point> t1, List<Point> t2){
		// current iteration
		int size_t1 = t1.size();
		int size_t2 = t2.size();

		// recursive stop conditions
		if(size_t1 == 0){
			double cost = 0;
			for(Point p : t2){
				cost += p.y;
			}
			costMatrix[0][t2.size()] = cost;
			return cost;
		}
		if(size_t2 == 0){
			double cost = 0;
			for(Point p : t1){
				cost += p.y;
			}
			costMatrix[t1.size()][0] = cost;
			return cost;
		}

		// recursive calls (for non-calculated nodes)
		double cost_a=0, cost_b=0, cost_c=0;
		if(costMatrix[size_t1 - 1][size_t2 - 1] == -1){
			cost_a = getERP(rest(t1), rest(t2)) + cost(t1.get(0), t2.get(0));
		} 
		if(costMatrix[size_t1 - 1][size_t2] == -1){ // gap
			cost_b = getERP(rest(t1), t2) + cost(t1.get(0)); 
		}
		if(costMatrix[size_t1][size_t2 - 1] == -1){ // gap
			cost_c = getERP(t1, rest(t2)) + cost(t2.get(0)); 
		}
		
		double min = min(cost_a, cost_b, cost_c);
		costMatrix[size_t1][size_t2] = min;
		
		return min;
	}*/
}
