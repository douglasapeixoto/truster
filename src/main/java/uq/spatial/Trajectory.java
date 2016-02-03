package uq.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import uq.spatial.distance.TrajectoryDistanceCalculator;

/**
 * A trajectory entity.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class Trajectory implements Serializable, Cloneable, Writable, GeoInterface {
	// the list of Points that composes this trajectory
	private List<Point> pointsList = 
			new ArrayList<Point>();
	
	/**
	 * This trajectory Identifier.
	 */
	public String id = ""; 
	
	public Trajectory(){}
	public Trajectory(String id){
		this.id = id;
	}

	/**
	 * Comparator to sort trajectory points by time-stamp.
	 */
	private Comparator<Point> timeComparator = 
			new TimeComparator<Point>(); 
	
	/**
	 * Sort this trajectory's sample points by time-stamp
	 * in ascending order.
	 */
	public void sort(){
		Collections.sort(pointsList, timeComparator);
	}
	
	/**
	 * The list of Points of this trajectory.
	 */
	public List<Point> getPointsList() {
		return pointsList;
	}
	
	/**
	 * The list of spatial-temporal segments in this trajectory.
	 */
	public List<STSegment> getSegmentsList() {
		List<STSegment> segList = 
				new ArrayList<STSegment>();
		if(pointsList.size() < 2){
			return segList;
		}
		for(int i=0; i<pointsList.size()-1; i++){
			Point p1 = pointsList.get(i);
			Point p2 = pointsList.get(i+1);
			segList.add(new STSegment(p1, p2));
		}
		return segList;
	}
	
	/**
	 * Add a point to this trajectory (end). 
	 */
	public void addPoint(Point point){
		pointsList.add(point);
	}

	/**
	 * Add a segment to this trajectory (end). 
	 */
	public void addSegment(STSegment s){
		pointsList.add(new Point(s.x1, s.y1, s.t1));
		pointsList.add(new Point(s.x2, s.y2, s.t2));
	}
	
	/**
	 *  Add a Point List to this trajectory (end). 
	 */
	public void addPointList(List<Point> pointsList){
		this.pointsList.addAll(pointsList);
	}
	
	/** 
	 * Removes the first occurrence of the specified point from 
	 * this trajectory, if it is present. 
	 * If this list does not contain the element, it is unchanged. 
	 * Returns true if this list contained the specified element.
	 */
	public boolean removePoint(Point p){
		return pointsList.remove(p);
	}
	
	/**
	 * Removes the point at the specified position in this trajectory. 
	 * Shifts any subsequent points to the left.
	 */
	public void removePoint(int index){
		assert(index >= 0 && index < this.size()) 
		: "Trajectory index out of bound";
		pointsList.remove(index);
	}

	/**
	 * Remove consecutive duplicated points from this 
	 * trajectory (if there is any).
	 * Note that duplicate points are checked under equals() 
	 * Point function over consecutive points only.
	 * 
	 * @return Return this updated trajectory.
	 */
	public Trajectory removeDuplicates(){
		// nothing to remove
		if(size()<=1){
			return this;
		}
		// new points list
		List<Point> auxPointsList = 
				new ArrayList<Point>();
		// add first point
		Point previous = pointsList.get(0);
		auxPointsList.add(previous);
		for(int i=1; i<size(); i++){
			Point current = pointsList.get(i);
			if(!current.equals(previous)){
				auxPointsList.add(current);
			}
			previous = current;
		}
		// update
		pointsList = auxPointsList;
		return this;
	}
	/**
	 * Merge two trajectories.
	 * Appends the trajectory t to the end of this trajectory.
	 */
	public void merge(Trajectory t){
		pointsList.addAll(t.getPointsList());
	}
	
	/**
	 * The distance (similarity) between these two trajectory.
	 * </br>
	 * Must provide the distance measure to use. 
	 */
	public double dist(Trajectory t, TrajectoryDistanceCalculator distanceMeasure){
		return distanceMeasure.getDistance(this, t);
	}
	
	/**
	 * Return the i-th point of this trajectory.
	 * Trajectory index from 0 (zero) to size - 1.
	 */
	public Point get(int i){
		assert(i >= 0 && i < this.size()) 
		: "Trajectory index out of bound";
		return pointsList.get(i);
	}
	
	/**
	 * Return the size of the trajectory. 
	 * Number of sample points.
	 */
	public int size(){
		return pointsList.size();
	}
	
	/**
	 * True if the trajectory contains no element.
	 */
	public boolean isEmpty(){
		return pointsList.isEmpty();
	}
	
	/**
	 * Return the initial time of this trajectory.
	 * Time stamp of the first sample point.
	 */
	public long timeIni(){
		if(!pointsList.isEmpty()){
			return first().time;
		}
		return 0;
	}
	
	/**
	 * Return the final time of this trajectory.
	 * Time stamp of the last sample point.
	 */
	public long timeEnd(){
		if(!pointsList.isEmpty()){
			return last().time;
		}
		return 0;
	}

	/**
	 * Return the length of this trajectory.
	 * Sum of the Euclidean distances between every point.
	 */
	public double length(){
		if(!isEmpty()){
			double length=0.0;
			for(int i=0; i<size()-1; i++){
				length += get(i).dist(get(i+1));
			}
			return length;	
		}
		return 0.0;
	}
	
	/**
	 * Return the time duration of this trajectory.
	 * Time taken from the beginning to the end of the
	 * trajectory.
	 */
	public long duration(){
		if(!this.isEmpty()){
			return (this.timeEnd() - this.timeIni());
		}
		return 0;
	}
	
	/**
	 * Return the average speed of this trajectory
	 * on a sphere surface (Earth).
	 */
	public double speed(){
		if(!this.isEmpty() && this.duration()!=0){
			return (this.length() / this.duration());
		}
		return 0.0;
	}
	
	/**
	 * Return the average sampling rate of the points in 
	 * this trajectory (average time between every sample
	 * point).
	 */
	public double samplingRate(){
		if(!this.isEmpty()){
			double rate=0.0;
			for(int i=0; i<pointsList.size()-1; i++){
				Point pi = pointsList.get(i);
				Point pj = pointsList.get(i+1);
				rate += pj.time - pi.time;
			}
			return (rate / (this.size()-1));
		}
		return 0.0;
	}
	
	/**
	 * The first sample point of this trajectory.
	 */
	public Point first(){
		if(!this.isEmpty()){
			return pointsList.get(0);
		}
		return null;
	}
	
	/**
	 * The last sample point of this trajectory.
	 */
	public Point last(){
		if(!this.isEmpty()){
			return pointsList.get(pointsList.size()-1);
		}
		return null;
	}

	/**
	 * Return a sub-trajectory of this trajectory, from 
	 * beginIndex inclusive to endIndex exclusive.
	 * </br>
	 * Note: trajectory index starts from 0 (zero).
	 */
	public Trajectory subTrajectory(int beginIndex, int endIndex){
		assert(beginIndex >= 0 && endIndex <= size() && 
			   beginIndex < endIndex)
		: "Trajectory index out of bound.";
		Trajectory sub = new Trajectory(id);
		sub.addPointList(pointsList.subList(beginIndex, endIndex));
		return sub;
	}
	
	/**
	 * Return the Minimum Boundary Rectangle (MBR) 
	 * of this trajectory.
	 */
	public Rectangle mbr(){
		if(!isEmpty()){
			double minX=MAX_X, maxX=MIN_X;
			double minY=MAX_Y, maxY=MIN_Y;  
			for(Point p : pointsList){
				if(p.x > maxX) maxX = p.x;
				if(p.x < minX) minX = p.x;
				if(p.y > maxY) maxY = p.y;
				if(p.y < minY) minY = p.y;
			}
			return new Rectangle(minX,maxX,minY,maxY);	
		}
		return new Rectangle(0.0,0.0,0.0,0.0);
	}

	/**
	 * Check if these trajectories intersect each other
	 * (Euclidean space only).
	 * </br>
	 * If the trajectories only touch edges or vertexes, 
	 * then also returns false.
	 */
	public boolean intersect(Trajectory t){
		if(this.isEmpty() || t.isEmpty()){
			return false;
		}
		for(int i=0; i < pointsList.size()-1; i++){
			Point i1 = pointsList.get(i);
			Point i2 = pointsList.get(i+1);
			Segment si = new Segment(i1.x, i1.y, i2.x, i2.y);
			for(int j=0; j < t.size()-1; j++){
				Point j1 = t.get(j);
				Point j2 = t.get(j+1);
				Segment sj = new Segment(j1.x, j1.y, j2.x, j2.y);
				if(si.intersect(sj)){
					return true;
				}
			}
		}
	    return false;
	}
	
	/**
	 * Check if this trajectory intersects with the given 
	 * line segment (Euclidean space only).
	 * </br>
	 * If the segment only touches edges or vertexes 
	 * of the trajectory, then also returns false.
	 */
	public boolean intersect(STSegment s){
		if(this.isEmpty() || s==null){
			return false;
		}
		for(int i=0; i < pointsList.size()-1; i++){
			Point p1 = pointsList.get(i);
			Point p2 = pointsList.get(i+1);
			Segment si = new Segment(p1.x, p1.y, p2.x, p2.y);
			if(si.intersect(s)){
				return true;
			}
		}
	    return false;
	}
	
	/**
	 * Print this trajectory: System out.
	 */
	public void print(){
		System.out.println(id + ": {");
		for(Point p : pointsList){
			p.print();
		}
		System.out.println("};");
	}
	
    /**
     * Makes an identical copy of this element
     */
    @Override
    public Trajectory clone() {
		Trajectory t_clone = new Trajectory();
		for(Point p : pointsList){
			Point new_p = p.clone();
			t_clone.addPoint(new_p);
		}
		return t_clone;
    }
    
	@Override
    public boolean equals(Object ob) {
        if (ob instanceof Trajectory) {
           Trajectory traj = (Trajectory) ob;
           return traj.id.equals(this.id);
        }
        return false;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public String toString() {
		StringBuilder toString = new StringBuilder();
		toString.append(id);
		for(Point p : pointsList){
			toString.append(" " + p.toString());
		}
		return toString.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		id = in.readLine();
	    int size = in.readInt();
	    pointsList = new ArrayList<Point>();//(size);
	    for(int i = 0; i < size; i++){
	        Point p = new Point();
	        p.readFields(in);
	        pointsList.add(p);
	    }
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeChars(id);//(pointsList.size());
	    out.writeInt(pointsList.size());
	    for(Point p : pointsList) {
	        p.write(out);
	    }
	}
}
