package uq.truster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.Rectangle;

/**
 * A grid object made of n x m rectangles.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class GridMatrix implements Serializable {
	// grid boxes (rectangles)
	private Rectangle[][] grid;
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
	public GridMatrix(int n, int m, double minX, double minY, double maxX, double maxY){
		this.minX = minX;
		this.minY = minY;
		this.maxX = maxX;
		this.maxY = maxY;
		this.sizeX = n;
		this.sizeY = m;
		grid = new Rectangle[sizeX][sizeY];
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
				grid[x][y] = new Rectangle(currentX, currentX+incrX, currentY, currentY+incrY);
				currentY += incrY;
			}
			currentX += incrX;
		}
	}
	
	/**
	 * The matrix containing the rectangles in this grid.
	 */
	public Rectangle[][] getRectangles(){
		return grid;
	}
	
	/**
	 * Return the rectangle in the x,y position.
	 */
	public Rectangle get(int x, int y){
		assert(x >= 0 && x < sizeX && y >= 0 && y < sizeY)
		: "Grid index out of bound";
		return  grid[x][y];
	}
	
	/**
	 * Number of rectangles in this grid.
	 */
	public int size(){
		return (sizeX*sizeY);
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
	 * Return the positions [x][y] of the rectangles in this grid that
	 * overlap with the given rectangular area. 
	 * </br>
	 * int[0] = x, int[1] = y.
	 */
	public List<int[]> getOverlappingRectangles(Rectangle r){
		List<int[]> posList = new ArrayList<int[]>();
		for(int x=0; x<sizeX; x++){
			for(int y=0; y<sizeY; y++){
				if(grid[x][y].overlap(r)){
					posList.add(new int[]{x,y});
				}
			}
		}
		return posList;
	}
	
	/**
	 * Print grid dimensions: system out
	 */
	public void print(){
		System.out.println("Grid Dimension: [" + sizeX + " x " + sizeY + "]\n");
		for(int y=sizeY-1; y>=0; y--){
			for(int x=0; x<sizeX; x++){
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",grid[x][y].minX,grid[x][y].maxY,grid[x][y].maxX,grid[x][y].maxY);
			}	
			System.out.println();
			for(int x=0; x<sizeX; x++){
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",grid[x][y].minX,grid[x][y].minY,grid[x][y].maxX,grid[x][y].minY);
			}
			System.out.println("\n");
		}
		
	}
}
