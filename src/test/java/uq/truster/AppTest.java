package uq.truster;

import java.util.List;

import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.Rectangle;
import uq.spatial.Segment;

/**
 * Unit test for simple App.
 */
public class AppTest {
	
	public static void main(String[] args) {
		/*Grid grid = new Grid(5, 3, 0, 0, 5, 3);
		grid.print();
		System.out.println();
		
		Rectangle r8 = grid.get(8);
		r8.print();
		
		List<Integer> posList = 
				grid.getAdjacentRectangles(0, 1);
		System.out.println();
		for(Integer i : posList){
			System.out.println(i);
		}*/

		Grid grid = new Grid(4, 4, 0, 0, 40, 40);
		int pos = grid.getOverlappingCell(new Point(25, 15));
		System.out.println(pos);

	}

	/**
	 * Calculate the intersection point of the given line segments.
	 * @return
	 */
	public static Point getIntersection(Segment s1, Segment s2){
		double x1 = s1.x2 - s1.x1;
		double x2 = s2.x2 - s2.x1;
		double y1 = s1.y2 - s1.y1;
		double y2 = s2.y2 - s2.y1;

		// line coeficients
		double a = (s1.x1 * s1.y2) - (s1.y1 * s1.x2);
		double b = (s2.x1 * s2.y2) - (s2.y1 * s2.x2);
		double c = (y1 * x2) - (x1 * y2);
		
		// Intersection
		double x = (a * x2 - b * x1) / c;
		double y = (a * y2 - b * y1) / c;

		return new Point(x, y);
	}
}
