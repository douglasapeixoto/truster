package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A grid object made of n x m rectangles.
 * </br>
 * The grid is constructed from left to right, 
 * bottom to top. The first position in the grid
 * is the position index 0 (zero)=(0,0).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Grid implements Serializable {
	// grid rectangles
	private List<Rectangle> gridList;
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
		gridList = new ArrayList<Rectangle>(sizeX*sizeY);
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
				gridList.add(new Rectangle(currentX, currentY, currentX+incrX, currentY+incrY));
				currentX += incrX;
			}
			currentY += incrY;
		}
	}
	
	/**
	 * The list of rectangles in this grid.
	 */
	public List<Rectangle> getRectangles(){
		return gridList;
	}
	
	/**
	 * Return the i-th rectangle in this grid.
	 */
	public Rectangle get(final int i) {
		assert(i>=0 && i<size())
		: "Grid index out of bound.";
		return gridList.get(i);
	}
	
	/**
	 * Return the rectangle in this position [x,y] 
	 * in the grid. Grid x and y position start 
	 * from (0,0).
	 */
	public Rectangle get(final int x, final int y) {
		assert(x>=0 && x<sizeX && y>=0 && y<sizeY)
		: "Grid index out of bound.";
		int index = y*sizeX + x;
		return gridList.get(index);
	}
	
	/**
	 * Number of rectangles in this grid.
	 */
	public int size(){
		return gridList.size();
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
		for(int i=0; i<gridList.size(); i++){
			Rectangle rec = gridList.get(i);
			if(rec.overlap(r)){
				posList.add(i);
			}
		}
		return posList;
	}

	/**
	 * Return the positions (index) of the rectangles in this grid that
	 * overlaps with the given line segment, that is, the id of the
	 * rectangles that either contains or intersect the line segment
	 */
	public List<Integer> getOverlappingRectangles(Segment s) {
		List<Integer> posList = new ArrayList<Integer>();
		for(int i=0; i<gridList.size(); i++){
			Rectangle r = gridList.get(i);
			if(r.overlap(s)){
				posList.add(i);
			}
		}
		return posList;
	}
	
	/**
	 * Return the positions (index) of the adjacent rectangles
	 * from the given grid position. 
	 */
	public List<Integer> getAdjacentRectangles(final int x, final int y){
		assert(x>=0 && x<sizeX && y>=0 && y<sizeY)
		: "Grid index out of bound.";
		
		List<Integer> posList = new ArrayList<Integer>();
		int adjX, adjY;
		int index;

		adjX = x-1; adjY = y-1;
		if(adjX>=0 && adjY>=0){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x; adjY = y-1;
		if(adjY>=0){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x+1; adjY = y-1;
		if(adjX<sizeX && adjY>=0){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x-1; adjY = y;
		if(adjX>=0){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x+1; adjY = y;
		if(adjX<sizeX){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x-1; adjY = y+1;
		if(adjX>=0 && adjY<sizeY){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x; adjY = y+1;
		if(adjY<sizeY){
			index = adjY*sizeX + adjX;
			posList.add(index);
		}
		adjX = x+1; adjY = y+1;
		if(adjX<sizeX && adjY<sizeY){
			index = adjY*sizeX + adjX;
			posList.add(index);
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
				Rectangle r = gridList.get(index);
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",r.min_x,r.max_y,r.max_x,r.max_y);
			}	
			System.out.println();
			for(int x=0; x<sizeX; x++){
				int index = y*sizeX + x;
				Rectangle r = gridList.get(index);
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",r.min_x,r.min_y,r.max_x,r.min_y);
			}
			System.out.println("\n");
		}
	}
}
