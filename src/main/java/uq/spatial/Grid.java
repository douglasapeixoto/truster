package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A grid object made of n x m rectangles.
 * </br>
 * The grid is constructed from bottom to top, 
 * left to right. The first position is position 
 * index 0 (zero)=(0,0).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Grid implements Serializable {
	// grid boxes (rectangles)
	private List<Rectangle> grid;
	// grid endpoint coordinates
	public double minX;
	public double minY;
	public double maxX;
	public double maxY;
	// grid dimensions (number of rectangles)
	public int sizeX;
	public int sizeY;
	
	/**
	 * Create a new grid of (n x m) rectangles for the given dimensions. 
	 */
	public Grid(int n, int m, double minX, double minY, double maxX, double maxY){
		this.minX = minX;
		this.minY = minY;
		this.maxX = maxX;
		this.maxY = maxY;
		this.sizeX = n;
		this.sizeY = m;
		grid = new ArrayList<Rectangle>(sizeX*sizeY);
		// build the grid
		build();
	}
	
	/**
	 * Generates the grid partitions.
	 */
	private void build() {
		// increments
		double incrX = (maxX-minX) / sizeX;
		double incrY = (maxY-minY) / sizeY;
		double currentX, currentY=minY;
		currentY=minY;
		for(int y=0; y<sizeY; y++){	
			currentX=minX;
			for(int x=0; x<sizeX; x++){
				grid.add(new Rectangle(currentX, currentY, currentX+incrX, currentY+incrY));
				currentX += incrX;
			}
			currentY += incrY;
		}
	}
	
	/**
	 * The list of rectangles in this grid.
	 */
	public List<Rectangle> getRectangles(){
		return grid;
	}
	
	/**
	 * Return the i-th rectangle in this grid.
	 */
	public Rectangle get(int i) {
		assert(i>=0 && i<size())
		: "Grid index out of bound.";
		return grid.get(i);
	}
	
	/**
	 * Return the rectangle in this position [x,y] 
	 * in the grid. Grid x and y position start 
	 * from (0,0).
	 */
	public Rectangle get(int x, int y) {
		assert(x>=0 && x<sizeX && y>=0 && y<sizeY)
		: "Grid index out of bound.";
		int index = y*sizeX + x;
		return grid.get(index);
	}
	
	/**
	 * Number of rectangles in this grid.
	 */
	public int size(){
		return grid.size();
	}
	
	/**
	 * The total area covered by this grid.
	 */
	public double area(){
		return (maxX-minX)*(maxY-minY);
	}
	
	/**
	 * The total perimeter of this grid.
	 */
	public double perimeter(){
		return 2*(maxX-minX)+2*(maxY-minY);
	}
	
	/**
	 * Return the positions (index) of the rectangles in this grid that
	 * overlap with the given rectangular area. 
	 */
	public List<Integer> getOverlappingRectangles(Rectangle r){
		List<Integer> posList = new ArrayList<Integer>();
		int i=0;
		for(Rectangle rec : grid){
			if(rec.overlap(r)){
				posList.add(i);
			}
			i++;
		}
		return posList;
	}
	
	/**
	 * Print grid dimensions: system out
	 */
	public void print(){
		System.out.println("Grid Dimensions: [" + sizeX + " x " + sizeY + "]\n");
		for(int y=sizeY-1; y>=0; y--){
			for(int x=0; x<sizeX; x++){
				int index = y*sizeX + x;
				Rectangle r = grid.get(index);
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",r.minX,r.maxY,r.maxX,r.maxY);
			}	
			System.out.println();
			for(int x=0; x<sizeX; x++){
				int index = y*sizeX + x;
				Rectangle r = grid.get(index);
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",r.minX,r.minY,r.maxX,r.minY);
			}
			System.out.println("\n");
		}
	}
}
