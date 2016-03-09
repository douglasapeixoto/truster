package uq.truster;

import java.io.Serializable;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory;
import uq.spatial.distance.TrajectoryDistanceCalculator;

/**
* EDwP: Edit Distance with Projections.
* 
* </br> Spatial dimension only.
* </br> Robust to non-uniform sampling rates.
* </br> Robust to local time shifts.
* </br> Threshold free.
* </br> Not a metric.
*
* @author uqdalves
*/
@SuppressWarnings("serial")
public class EDwP_V1 implements Serializable, TrajectoryDistanceCalculator {

	public double getDistance(Trajectory t1, Trajectory t2) {
		// make sure the original trajectories will not be changed
		List<Point> r_clone = t1.clone().getPointsList();
		List<Point> s_clone = t2.clone().getPointsList();
		
		// get distance
		double dist = getEDwP(r_clone, s_clone);
		if(Double.isNaN(dist)){
			return INFINITY;
		}
		return dist;
	}
	
	/**
	 * Edit Distance with Projections
	 */
	private double getEDwP(List<Point> t1, List<Point> t2) {
		double edwp_cost = 0;
		
		if(t1.size() == 0 && t2.size() == 0){
			return 0;
		}
		if(t1.size() == 0 || t2.size() == 0){
			return INFINITY;
		}

		double replacement, coverage;
		double cost_e1, cost_e2;
		Point e1p1, e1p2, e2p1, e2p2;
		Point proj_e1, proj_e2;
		while(true) {
			// Segments being edited
			if (t2.size() == 1 && t1.size() > 1){
				e1p1 = t1.get(0);
				e1p2 = t1.get(1);
				e2p2 = t2.get(0);

				replacement = replacement(e1p1, e1p2, e2p2, e2p2);
				coverage    = coverage(e1p1, e1p2, e2p2, e2p2);
				edwp_cost += (replacement * coverage);
			}
			else if(t1.size() == 1 && t2.size() > 1){			
				e2p1 = t2.get(0);
				e2p2 = t2.get(1);
				e1p2 = t1.get(0);
			
				replacement = replacement(e2p1, e2p2, e1p2, e1p2);
				coverage    = coverage(e2p1, e2p2, e1p2, e1p2);
				edwp_cost += (replacement * coverage);
			} 
			else if(t1.size() > 1 && t2.size() > 1){
				e1p1 = t1.get(0);
				e1p2 = t1.get(1);
				e2p1 = t2.get(0);
				e2p2 = t2.get(1);
	
				// cost of project e2 onto e1
				proj_e1		= projection(e1p1, e1p2, e2p2);
				replacement = replacement(e1p1, proj_e1, e2p1, e2p2);
				coverage	= coverage(e1p1, proj_e1, e2p1, e2p2);
				cost_e1	    = replacement * coverage;

				// cost of project e1 onto e2
				proj_e2 	= projection(e2p1, e2p2, e1p2);
				replacement = replacement(e2p1, proj_e2, e1p1, e1p2);
				coverage	= coverage(e2p1, proj_e2, e1p1, e1p2);
				cost_e2	    = replacement * coverage;
				
				// check which edit is cheaper
				if(cost_e1 <= cost_e2){
					// replacement 1 is better
					edwp_cost += cost_e1;
					// update T1
					t1 = insert(t1, proj_e1);
				} else {
					// replacement 2 is better
					edwp_cost += cost_e2;
					// update T2
					t2 = insert(t2, proj_e2);
				}
			}
			// end
			else {
				break;
			}
			// move forward
			t1 = rest(t1);
			t2 = rest(t2);
		}
		
		return edwp_cost;
	}
	
	/**
	 * Cost of the operation where the segment e1 is matched with e2.
	 */
	private double replacement(Point e1_p1, Point e1_p2, Point e2_p1, Point e2_p2) {
		// Euclidean distances between segments' points
		double dist_p1 = e1_p1.dist(e2_p1);
		double dist_p2 = e1_p2.dist(e2_p2);

		// replacement cost
		double rep_cost = dist_p1 + dist_p2;
		
		return rep_cost;
	}
	
	/**
	 * Coverage; quantifies how representative the segment being
	 * edit are of the overall trajectory. Segment e1 and e2.
	 * e1 = [e1.p1, e1.p2], e2 = [e2.p1, e2.p2]
	 */
	private double coverage(Point e1_p1, Point e1_p2, Point e2_p1, Point e2_p2){
		// segments coverage
		double cover = e1_p1.dist(e1_p2) + e2_p1.dist(e2_p2);
		return cover;
	}
	
	/**
	 * Returns a sub-trajectory containing all segments of 
	 * the list except the first one.
	 */
	private List<Point> rest(List<Point> list){
		if(!list.isEmpty()){
			list.remove(0);
		}
		return list;	
	}

	/**
	 * Insert the projection p_ins on to the first segment of
	 * the trajectory t1.
	 *
	 * @return The new updates trajectory t1.
	 */
	private List<Point> insert(List<Point> t, Point p_ins){
		Point e1_p1 = t.get(0);
		Point e1_p2 = t.get(1);
		// if the projection is not already there,
		// then add the new point
		if(!p_ins.isSamePosition(e1_p1) && 
		   !p_ins.isSamePosition(e1_p2)){
			t.add(1, p_ins);
		}
		return t;
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
	
	/*
	public static void main(String[] arg){

		Trajectory t1 = new Trajectory("T1");
		t1.addPoint(new Point(0, 0, 0));
		t1.addPoint(new Point(0, 10, 30));
		t1.addPoint(new Point(0, 12, 35));
		
		
		Trajectory t2 = new Trajectory("T2");
		t2.addPoint(new Point(2, 0, 0));
		t2.addPoint(new Point(2, 7, 14));
		t2.addPoint(new Point(2, 10, 17));
				
		EDwPDistanceCalculator ed = new EDwPDistanceCalculator();
		double dist = ed.getDistance(t1, t2);//getDistance(t1, t2);
		System.out.println("Dist: " + dist);
	}*/
}
