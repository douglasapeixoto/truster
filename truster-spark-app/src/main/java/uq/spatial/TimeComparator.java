package uq.spatial;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator to sort objects by time-stamp.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class TimeComparator<T> implements Serializable, Comparator<T>{
	
	/**
	 * Compare objects (Points or Trajectories) by time stamp 
	 * in ascending order.
	 */
	public int compare(T obj1, T obj2) {
		if(obj1 instanceof Point){
			Point p1 = (Point)obj1; 
			Point p2 = (Point)obj2;
			return compare(p1, p2);
		} 
		if(obj1 instanceof Trajectory){
			Trajectory t1 = (Trajectory)obj1;
			Trajectory t2 = (Trajectory)obj2;
			return compare(t1, t2);
		}
		if(obj1 instanceof STSegment){
			STSegment s1 = (STSegment)obj1;
			STSegment s2 = (STSegment)obj2;
			return compare(s1, s2);
		}
		return 0;
	}
	
	/**
	 * Compare points by time-stamp in ascending order.
	 */
	private int compare(Point p1, Point p2) {
		return p1.time > p2.time ? 1 : (p1.time < p2.time ? -1 : 0);
	}
	
	/**
	 * Compare trajectories by initial time-stamp in ascending order.
	 */
	private int compare(Trajectory t1, Trajectory t2) {
		return t1.timeIni() > t2.timeIni() ? 1 : (t1.timeIni() < t2.timeIni() ? -1 : 0);
	}
	
	/**
	 * Compare spatial-temporal segments by initial time-stamp in ascending order.
	 */
	private int compare(STSegment s1, STSegment s2) {
		return s1.t1 > s2.t1 ? 1 : (s1.t1 < s2.t1 ? -1 : 0);
	}
	
}
