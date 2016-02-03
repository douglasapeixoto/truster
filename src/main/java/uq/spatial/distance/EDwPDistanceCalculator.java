package uq.spatial.distance;

import java.io.Serializable; 
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
 
import uq.spatial.Point; 
import uq.spatial.TimeComparator;
import uq.spatial.Trajectory;

/**
* EDwP: Edit Distance with Projections.
* 
* </br> Spatial dimension only.
* </br> Robust to non-uniform sampling rates.
* </br> Robust to local time shifts.
* </br> Threshold free.
* </br> Not a metric.
* </br> Complexity: O((|T1|+|T2|)^2)
* 
* @author uqdalves
*/
@SuppressWarnings("serial")
public class EDwPDistanceCalculator implements Serializable, TrajectoryDistanceCalculator {
	
	public double getDistance(Trajectory t1, Trajectory t2) {
		// make sure the original trajectories will not be changed
		List<Point> t1_clone = t1.clone().getPointsList();
		List<Point> t2_clone = t2.clone().getPointsList();

		return getEDwP(t1_clone, t2_clone);
	}
	
	private double getEDwP(List<Point> t1, List<Point> t2) {
		if(t1.size() == 0 && t2.size() == 0){
			return 0.0;
		}
		if(t1.size() == 0 || t2.size() == 0){
			return INFINITY;
		}

		// project T2 onto T1
		List<Point> t1_with_projections = project(t2, t1);
		// project T1 onto T2
		List<Point> t2_with_projections = project(t1, t2);

		// both lists will have same size after projections
		int size = t1_with_projections.size();

		// coverage and replacement
		double coverage, replacement;
		double cost;
		
		// check which edit was cheaper
		double edwp_cost = 0.0;
		for(int i=0; i<size-1; i++){
			// segment e1
			Point e1p1 = t1_with_projections.get(i);
			Point e1p2 = t1_with_projections.get(i+1);
			// segment e2
			Point e2p1 = t2_with_projections.get(i);
			Point e2p2 = t2_with_projections.get(i+1);
			
			// check if the segments are not already aligned
			coverage = coverage(e1p1, e1p2, e2p1, e2p2);
			if(coverage == 0.0) continue;
			
			// test t1 onto t2
			double min_cost_e1 = INFINITY;
			for(int j=0; j<t2.size()-1; j++){
				Point p1 = t2.get(j);
				Point p2 = t2.get(j+1);
				
				replacement = replacement(e1p1, e1p2, p1, p2);
				coverage    = coverage(e1p1, e1p2, p1, p2);

				cost = coverage * replacement;
				if(cost < min_cost_e1 && cost != 0.0){
					min_cost_e1 = cost;
				}
			}

			// test t2 onto t1
			double min_cost_e2 = INFINITY;
			for(int j=0; j<t1.size()-1; j++){
				Point p1 = t1.get(j);
				Point p2 = t1.get(j+1);
				
				replacement = replacement(e2p1, e2p2, p1, p2);
				coverage    = coverage(e2p1, e2p2, p1, p2);

				cost = coverage * replacement;
				if(cost < min_cost_e2){
					min_cost_e2 = cost;
				}
			}
			
			// get the cheapest edit
			edwp_cost += Math.min(min_cost_e1, min_cost_e2);
		}
		
		return edwp_cost;	
	}

	/**
	 * Project all points from t1 onto t2.
	 * </br>
	 * Find the cheapest projection of each point (shortest distance).
	 * 
	 * @return Return a copy of t2 with the new projections from t1.
	 */
	private List<Point> project(List<Point> t1, List<Point> t2){
		List<Point> projList = 
				new ArrayList<Point>();
		// find the best projection of every point in t1 onto t2
		double dist;
		Point ep1, ep2, proj;
		for(int i=0; i<t1.size(); i++){
			Point p = t1.get(i);
			double minDist = INFINITY;
			Point bestProj = new Point();
			for(int j=0; j<t2.size()-1; j++){
				// t2 segment
				ep1 = t2.get(j);
				ep2 = t2.get(j+1);
				// projection of p onto e
				proj = projection(ep1, ep2, p);
				// find the best pro
				dist = p.dist(proj);
				if(dist < minDist){
					minDist = dist;
					bestProj = proj;
				}
			}
			projList.add(bestProj);
		}
		// update and sort t2
		for(Point p : t2){
			projList.add(p);
		}
		Collections.sort(projList, new TimeComparator<Point>());
		
		return projList;
	}
	
	/**
	 * Cost of the operation where the segment e1 is matched with e2.
	 */
	private double replacement(Point e1_p1, Point e1_p2, Point e2_p1, Point e2_p2) {
		// Euclidean distances between segment points
		double dist_p1 = e1_p1.dist(e2_p1);
		double dist_p2 = e1_p2.dist(e2_p2);

		// replacement cost
		double rep_cost = dist_p1 + dist_p2;
		
		return rep_cost;
	}
	
	/**
	 * Coverage: quantifies how representative the segment being
	 * edit are of the overall trajectory. Segment e1 and e2.
	 * e1 = [e1.p1, e1.p2], e2 = [e2.p1, e2.p2]
	 */
	private double coverage(Point e1_p1, Point e1_p2, Point e2_p1, Point e2_p2){
		// segments coverage
		double cover = e1_p1.dist(e1_p2) + e2_p1.dist(e2_p2);
		return cover;
	}
		
	/**
	 * Calculate the projection of the point p on to the segment
	 * e = [e.p1, e.p2]
	 */
	private Point projection(Point e_p1, Point e_p2, Point p){
		// square length of the segment
		double len_2 = dotProduct(e_p1, e_p2, e_p1, e_p2);
		// e.p1 and e.p2 are the same point
		if(len_2 == 0){
			return e_p1;
		}
	    
	    // the projection falls where t = [(p-e.p1) . (e.p2-e.p1)] / |e.p2-e.p1|^2
	    double t = dotProduct(e_p1, p, e_p1, e_p2) / len_2;
	    
	    // "Before" e.p1 on the line
	    if (t < 0.0) {
	    	return e_p1;
	    }
	    // after e.p2 on the line 
	    if(t > 1.0){
	    	return e_p2;
	    }
	    // projection is "in between" e.p1 and e.p2
	    // get projection coordinates
	    double x = e_p1.x + t*(e_p2.x - e_p1.x);
	    double y = e_p1.y + t*(e_p2.y - e_p1.y);
	    long time = (long) (e_p1.time + t*(e_p2.time - e_p1.time));
	   
	    return new Point(x, y, time);
	}
	
	/**
	 * Calculates the dot product between two segment e1 and e2.
	 */
	private double dotProduct(Point e1_p1, Point e1_p2, Point e2_p1, Point e2_p2){
		// shift the points to the origin - vector
		double e1_x = e1_p2.x - e1_p1.x;
		double e1_y = e1_p2.y - e1_p1.y;
		double e2_x = e2_p2.x - e2_p1.x;
		double e2_y = e2_p2.y - e2_p1.y;

		// calculate the dot product
		double dot_product = (e1_x * e2_x) + (e1_y * e2_y);
		
		return dot_product;
	}

	/*public static void main(String[] arg){

		Trajectory t1 = new Trajectory("T1");
		t1.addPoint(new Point(0, 0, 0));
		t1.addPoint(new Point(0, 10, 30));
		t1.addPoint(new Point(0, 12, 35));

		Trajectory t2 = new Trajectory("T2");
		t2.addPoint(new Point(2, 0, 0));
		t2.addPoint(new Point(2, 7, 14));
		t2.addPoint(new Point(2, 10, 17));

		EDwPDistanceCalculator ed = new EDwPDistanceCalculator();

		double dist = ed.getDistance(t1, t2); 
		System.out.println("Dist: " + dist);
	}*/

}
