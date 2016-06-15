package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A 2D rectangle object whose edges  
 * are parallel to the X and Y axis.
 * 
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class Rectangle implements Serializable, GeoInterface {
	// X and Y axis position
	public double min_x;
	public double min_y;
	public double max_x;
	public double max_y;
	
	public Rectangle(){}
	public Rectangle(double min_x, double min_y, double max_x, double max_y) {
		this.min_x = min_x;
		this.min_y = min_y;
		this.max_x = max_x;
		this.max_y = max_y;
	}

	/**
	 * The perimeter of this rectangle
	 */
	public double perimeter(){
		return (2*Math.abs(max_y-min_y) + 2*Math.abs(max_x-min_x));
	}

	/**
	 * The area of this rectangle
	 */
	public double area(){
		return (max_y-min_y)*(max_x-min_x);
	}
	
	/**
	 * Returns the center of this rectangle as a coordinate point.
	 */
	public Point center(){
		double xCenter = min_x + (max_x - min_x)/2;
		double yCenter = min_y + (max_y - min_y)/2; 
		return new Point(xCenter, yCenter);
	}

	/**
	 * Check is this rectangle contains the given point inside its perimeter.
	 * Check if the point lies totally inside the rectangle area.
	 */
	public boolean contains(Point p){
		return contains(p.x, p.y);
	}
	
	/**
	 * Check is this rectangle contains the given point inside its perimeter.
	 * Check if the point lies totally inside the rectangle area.
	 * </br>
	 * Point given by X and Y coordinates
	 */
	public boolean contains(double x, double y){
		if(x > min_x && x < max_x &&
		   y > min_y && y < max_y){
			return true;
		}
		return false;
	}

	/**
	 * Check is this rectangle contains the given line segment,
	 * that is, line segment is inside the rectangle area.
	 */
	public boolean contains(Segment s){
		return contains(s.x1, s.y1, s.x2, s.y2);
	}
	
	/**
	 * Check is this rectangle contains the given line segment,
	 * that is, line segment is inside the rectangle area.
	 * </br>
	 * Line segment given by end points X and Y coordinates.
	 */
	public boolean contains(double x1, double y1, double x2, double y2){
		if((contains(x1, y1) || touch(x1, y1)) && 
		   (contains(x2, y2) || touch(x2, y2))){
			return true;
		}
		return false;
	}	
	
	/**
	 * Check is the specified point touches 
	 * any of the rectangle edges.
	 */
	public boolean touch(Point p){
		return touch(p.x, p.y);
	}
	
	/**
	 * Check is the specified point touches 
	 * any of the rectangle edges.
	 * </br>
	 * Point given by X and Y coordinates.
	 */
	public boolean touch(double x, double y){
		// check top and bottom edges
		if((y == max_y || y == min_y) &&
		    x >= min_x && x <= max_x){
			return true;
		}
		// check left and right edges
		if((x == min_x || x == max_x) &&
			y >= min_y && y <= max_y){
			return true;
		}
		return false;
	}

	/**
	 * Check is these two rectangles overlap.
	 */
	public boolean overlap(Rectangle r){
		if(this.max_x < r.min_x) return false;
		if(this.min_x > r.max_x) return false;
		if(this.max_y < r.min_y) return false;
		if(this.min_y > r.max_y) return false;
		return true;
	}
	
	/**
	 * Check if the given line segment overlaps with this rectangle, 
	 * that is, check if this rectangle either contains or intersect 
	 * the given line segment.
	 */
	public boolean overlap(Segment s){
		if(contains(s) || intersect(s)){
			return true;
		}
		return false;
	}

	/**
	 * Check if the given line segment overlaps with this rectangle, 
	 * that is, check if this rectangle either contains or intersect
	 * the given line segment.
	 * </br>
	 * Line segment given by endpoint coordinates.
	 */
	public boolean overlap(double x1, double y1, double x2, double y2){
		if(contains(x1, y1, x2, y2) || intersect(x1, y1, x2, y2)){
			return true;
		}
		return false;
	}
	
	/**
	 * Check if the given line segment intersects this rectangle.
	 */
	public boolean intersect(Segment s){
		return intersect(s.x1, s.y1, s.x2, s.y2);
	}

	/**
	 * Check if the given line segment intersects this rectangle.
	 * </br>
	 * Line segment is given by end point coordinates.
	 */
	public boolean intersect(double x1, double y1, double x2, double y2){
		// check box LEFT edge
		Segment edge = new Segment(min_x, min_y, min_x, max_y);
		if(edge.intersect(x1, y1, x2, y2)){
			return true;
		}
		// check RIGHT edge
		edge = new Segment(max_x, min_y, max_x, max_y);
		if(edge.intersect(x1, y1, x2, y2)){
			return true;
		}
		// check TOP edge
		edge = new Segment(min_x, max_y, max_x, max_y);
		if(edge.intersect(x1, y1, x2, y2)){
			return true;
		}
		// check BOTTOM edge
		edge = new Segment(min_x, min_y, max_x, min_y);
		if(edge.intersect(x1, y1, x2, y2)){
			return true;
		}
		// no intersection
	    return false;
	}
	
	/**
	 * The shortest distance between these two rectangles.
	 * </br>
	 * The distance is the shortest distance between 
	 * the pairwise edges.
	 */
	public double dist(Rectangle r){
		double min_dist = INF;
		double dist;
		for(Segment si : this.getEdgeList()){
			for(Segment sj : r.getEdgeList()){
				dist = si.dist(sj);
				if(dist < min_dist){
					min_dist = dist;
				}
			}
		}
		return min_dist;
	}

	/**
	 * The shortest distance between this rectangle
	 * and the given line segment.
	 * </br>
	 * The distance is the shortest distance between 
	 * the given segment and the rectangle edges.
	 */
	public double dist(Segment s){
		double min_dist = INF;
		double dist;
		for(Segment si : this.getEdgeList()){
			dist = si.dist(s);
			if(dist < min_dist){
				min_dist = dist;
			}
		}
		return min_dist;
	}
	
	/**
	 * The shortest distance between this rectangle
	 * and the given point.
	 * </br>
	 * The distance is the shortest distance between 
	 * the given point and the rectangle edges.
	 */
	public double dist(Point p){
		double min_dist = INF;
		double dist;
		for(Segment si : this.getEdgeList()){
			dist = si.dist(p.x, p.y);
			if(dist < min_dist){
				min_dist = dist;
			}
		}
		return min_dist;
	}
	
	/**
	 * Return the four vertexes of this rectangle
	 * as coordinate points.
	 */
	public List<Point> getVertexList(){
		List<Point> corners = new ArrayList<Point>();
		Point p1 = new Point(min_x, max_y);
		Point p2 = new Point(max_x, max_y);
		Point p3 = new Point(min_x, min_y);
		Point p4 = new Point(max_x, min_y);
		corners.add(p1);	corners.add(p2);
		corners.add(p3);	corners.add(p4);
		
		return corners;
	}
	
	/**
	 * Return the four edges of this rectangle
	 * as line segments.
	 */
	public List<Segment> getEdgeList(){
		List<Segment> edges = new ArrayList<Segment>();
		Segment e1 = new Segment(min_x, min_y, max_x, min_y); // bottom
		Segment e2 = new Segment(min_x, min_y, min_x, max_y); // left
		Segment e3 = new Segment(min_x, max_y, max_x, max_y); // top
		Segment e4 = new Segment(max_x, min_y, max_x, max_y); // right
		edges.add(e1); edges.add(e2);
		edges.add(e3); edges.add(e4);
		
		return edges;
	}
	
	/**
	 * Left edge.
	 */
	public Segment leftEdge(){
		return new Segment(min_x, min_y, min_x, max_y);
	}
	
	/**
	 * Right edge.
	 */
	public Segment rightEdge(){
		return new Segment(max_x, min_y, max_x, max_y);
	}
	
	/**
	 * Bottom edge.
	 */
	public Segment lowerEdge(){
		return new Segment(min_x, min_y, max_x, min_y);
	}
	
	/**
	 * Top edge.
	 */
	public Segment upperEdge(){
		return new Segment(min_x, max_y, max_x, max_y);
	}
	
	/**
	 * Print this rectangle: System out.
	 */
	public void print(){
		System.out.println("Rectangle:");
		System.out.format("[(%.2f,%.2f) (%.2f,%.2f)]",min_x,max_y,max_x,max_y);
		System.out.println();
		System.out.format("[(%.2f,%.2f) (%.2f,%.2f)]",min_x,min_y,max_x,min_y);
		System.out.println();
		System.out.println("Area: " + area());
		System.out.println("Perimeter: " + perimeter());
	}
}
