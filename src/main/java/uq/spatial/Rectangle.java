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
public class Rectangle implements Serializable {
	// X and Y axis position
	public double minX;
	public double minY;
	public double maxX;
	public double maxY;
	
	public Rectangle(){}
	public Rectangle(double minX, double minY, double maxX, double maxY) {
		this.minX = minX;
		this.minY = minY;
		this.maxX = maxX;
		this.maxY = maxY;
	}

	/**
	 * The perimeter of this rectangle
	 */
	public double perimeter(){
		return (2*Math.abs(maxY-minY) + 2*Math.abs(maxX-minX));
	}
	
	/**
	 * The area of this rectangle
	 */
	public double area(){
		return (maxY-minY)*(maxX-minX);
	}
	
	/**
	 * Returns the center of this rectangle as a coordinate point.
	 */
	public Point center(){
		double xCenter = minX + (maxX - minX)/2;
		double yCenter = minY + (maxY - minY)/2; 
		return new Point(xCenter, yCenter);
	}

	/**
	 * True is this rectangle contains the given point inside its perimeter.
	 * Check if the point lies inside the rectangle area.
	 */
	public boolean contains(Point p){
		return contains(p.x, p.y);
	}
	
	/**
	 * True is this rectangle contains the given point inside its perimeter.
	 * Check if the point lies inside the rectangle area.
	 * Point given by X and Y coordinates
	 */
	public boolean contains(double x, double y){
		if(x >= minX && x <= maxX &&
		   y >= minY && y <= maxY){
			return true;
		}
		return false;
	}

	/**
	 * True is this rectangle contains the given line segment,
	 * that is, line segment totally inside the rectangle area.
	 * </br>
	 * Line segment given by end points X and Y coordinates.
	 */
	public boolean contains(double x1, double y1, double x2, double y2){
		if(contains(x1, y1) && contains(x2, y2)){
			return true;
		}
		return false;
	}	
	
	/**
	 * True is this rectangle touches the specified point.
	 * Check if the point touches the rectangle edges.
	 */
	public boolean touch(Point p){
		return touch(p.x, p.y);
	}
	
	/**
	 * True is this rectangle touches the specified point.
	 * Check if the point touches the rectangle edges.
	 * Point given by X and Y coordinates.
	 */
	public boolean touch(double x, double y){
		// check top and bottom edges
		if( x >= minX && x <= maxX && 
		   (y == maxY || y == minY) ){
			return true;
		}
		// check left and right edges
		if( y >= minY && y <= maxY && 
		   (x == minX || x == maxX) ){
			return true;
		}
		return false;
	}

	/**
	 * True is these two rectangles overlap.
	 */
	public boolean overlap(Rectangle r){
		if(this.maxX < r.minX) return false;
		if(this.minX > r.maxX) return false;
		if(this.maxY < r.minY) return false;
		if(this.minY > r.maxY) return false;
		return true;
	}
	
	/**
	 * Check if the given line segment intersects this rectangle.
	 * Line segment is given by end point coordinates.
	 * </br></br>
	 * If the line segment do not cross or only touches the
	 * rectangle edges or vertexes then return null.
	 * 
	 * @return Return the rectangle edge which intersect with the
	 * given line segment. If they do not intersect then return null.
	 */
	public Segment intersect(double x1, double y1, double x2, double y2){
		// check box LEFT edge
		Segment egde = new Segment(minX, minY, minX, maxY);
		if(egde.intersect(x1, y1, x2, y2)){
			return egde;
		}
		// check RIGHT edge
		egde = new Segment(maxX, minY, maxX, maxY);
		if(egde.intersect(x1, y1, x2, y2)){
			return egde;
		}
		// check TOP edge
		egde = new Segment(minX, maxY, maxX, maxY);
		if(egde.intersect(x1, y1, x2, y2)){
			return egde;
		}
		// check BOTTOM edge
		egde = new Segment(minX, minY, maxX, minY);
		if(egde.intersect(x1, y1, x2, y2)){
			return egde;
		}
		// no intersection
	    return null;
	}

	/**
	 * Return the coordinates of the four vertexes of this rectangle.
	 */
	public List<Point> getVertexList(){
		List<Point> corners = new ArrayList<Point>();
		Point p1 = new Point(minX, maxY);
		Point p2 = new Point(maxX, maxY);
		Point p3 = new Point(minX, minY);
		Point p4 = new Point(maxX, minY);
		corners.add(p1);	corners.add(p2);
		corners.add(p3);	corners.add(p4);
		
		return corners;
	}
	
	/**
	 * Print this rectangle: System out.
	 */
	public void print(){
		System.out.println("Rectangle:");
		System.out.format("(%.2f,%.2f) (%.2f,%.2f)",minX,maxY,maxX,maxY);
		System.out.format("(%.2f,%.2f) (%.2f,%.2f)",minX,minY,maxX,minY);
		System.out.println("Area: " + area());
		System.out.println("Perimeter: " + perimeter());
	}

}
