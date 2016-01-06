package uq.spatial.distance;

import java.io.Serializable;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory; 

/**
* Spatial temporal largest common sub-sequence distance. 
*
* @author uqhsu1, uqdalves 
*/
@SuppressWarnings("serial")
public class STLCSSDistanceCalculator implements Serializable, TrajectoryDistanceCalculator{

    double Distance;
	double Time;
    long startTime1;
    long startTime2;
    
	@Override
	public double getDistance(Trajectory t1, Trajectory t2) {
		// make sure the original trajectories will not be changed
		List<Point> r_clone = t1.clone().getPointsList();
		List<Point> s_clone = t2.clone().getPointsList();
		
		// Time range (parameters - given)
		// Time = getTimeEnd(r, s); ?????
		startTime1 = r_clone.get(0).time;
        startTime2 = s_clone.get(0).time;

        // get distance
		double dist = getSTLCSS(r_clone, s_clone);
		if(Double.isNaN(dist)){
			return INFINITY;
		}
		return dist;
	} 
	
	public STLCSSDistanceCalculator(double distanceThreshold, long timeIntervalThreshold){
		Distance = distanceThreshold;
        Time = timeIntervalThreshold;
	}

	private double getSTLCSS(List<Point> r, List<Point> s){

		double[][] LCSSMetric = new double[r.size() + 1][s.size() + 1];
		
		for (int i = 0; i <= r.size(); i++){
			LCSSMetric[i][0] = 0;
		}
		for (int i = 0; i <= s.size(); i++){
			LCSSMetric[0][i] = 0;
		}

		LCSSMetric[0][0] = 0;

		for (int i = 1; i <= r.size(); i++){
			for (int j = 1; j <= s.size(); j++){
				if (subcost(r.get(i - 1), s.get(j - 1)) == 1){
					LCSSMetric[i][j] = LCSSMetric[i - 1][j - 1] + 1;
				}else{
					LCSSMetric[i][j] = max(LCSSMetric[i][j - 1], LCSSMetric[i - 1][j]);
				}
				
			}
		}		
                
        double lcss= LCSSMetric[r.size()][s.size()];
        
        double distanceV=1-(lcss/Math.min(r.size(), s.size()));
		
		return distanceV;
	}
	
	private double max(double a, double b){
		if (a >= b){
			return a;
		}else{
			return b;
		}
	}
	
	private int subcost(Point p1, Point p2){
		boolean isSame = true;
		if(Math.abs(p1.x - p2.x) > Distance){
			isSame = false;
		}
		if(Math.abs(p1.y - p2.y) > Distance){
			isSame = false;
		}
   
        if(Math.abs((p1.time - startTime1)-(p2.time - startTime2)) > Time){
            isSame = false;
        }
		
		if(isSame){
			return 1;
		}
		return 0;
	}
    
	/**
	 *  Get final time period tn
	 */
	/*private long getTimeEnd(ArrayList<Point> r, ArrayList<Point> s){
		// Get the trajectory with earliest last point
		long tn = s.get(s.size()-1).time < r.get(r.size()-1).time ? 
				s.get(s.size()-1).time : r.get(r.size()-1).time;
		return tn;
	}*/
}




