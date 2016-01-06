package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A grid object made of n x m rectangles.
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
		// generate the grid
		generate();
	}
	
	/**
	 * Generates the grid partitions.
	 */
	private void generate() {
		// increments
		double incrX = (maxX-minX) / sizeX;
		double incrY = (maxY-minY) / sizeY;
		double currentX, currentY=minY;
		currentX=minX;
		for(int x=0; x<sizeX; x++){	
			currentY=minY;
			for(int y=0; y<sizeY; y++){
				grid.add(new Rectangle(currentX, currentX+incrX, currentY, currentY+incrY));
				currentY += incrY;
			}
			currentX += incrX;
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
}
