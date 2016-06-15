package uq.truster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.Rectangle;
import uq.spatial.Segment;
import uq.spatial.Trajectory;

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

		/*
		Grid grid = new Grid(4, 4, 0, 0, 40, 40);
		grid.print();
		// test point x grid
		int pos = grid.getOverlappingCell(new Point(25, 15));
		System.out.println(pos);
		System.out.println();
		// test grid x rectangle
		Rectangle r = new Rectangle(10.5, 10.5, 20.5, 30.5);
		HashSet<Integer> set = grid.getOverlappingCells(r);
		for(int i : set){
			System.out.println(i);
		}
		*/
		
		getMinMax();
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
	
	/**
	 * Read the uses cases for Nearest Neighbors queries.
	 */
	public static List<Trajectory> getMinMax(){
		File file = new File("C:/lol/nn-test-cases");
		List<Trajectory> list = new LinkedList<Trajectory>();
		try {
			BufferedReader buffer = new BufferedReader(
					new FileReader(file));
			// process lines
			String line;
			int id=1;
			double x, y;
			long time;
			double minX = 10000000;
			double minY = 10000000;
			double maxX = -1000000;
			double maxY = -1000000;
			while(buffer.ready()){
				line = buffer.readLine();
				if(line.length() > 4){
					String[] tokens = line.split(" ");
					// first tokens is the id
					for(int i=1; i<=tokens.length-3; i+=3){
						x = Double.parseDouble(tokens[i]);
						y = Double.parseDouble(tokens[i+1]);
						time = Long.parseLong(tokens[i+2]);

						if(x > maxX){
							maxX = x;
						}
						if(x < minX){
							minX = x;
						}
						if(y > maxY){
							maxY = y;
						}
						if(y < minY){
							minY = y;
						}
					}
				}
			}
			System.out.println("Min X: " + minX);
			System.out.println("Max X: " + maxX);
			System.out.println("Min Y: " + minY);
			System.out.println("Max Y: " + maxY);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return list;
	}
}
