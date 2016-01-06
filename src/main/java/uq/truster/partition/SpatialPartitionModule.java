package uq.truster.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.Rectangle;
import uq.spatial.Segment;
import uq.spatial.Trajectory;

/**
 * TRUSTER Spatial partitioning module.
 * </br>
 * Split trajectories into segments and map segments 
 * to partitions (grid) according to their spatial extent.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SpatialPartitionModule implements Serializable {

	/**
	 * MapReduce partitioning.
	 * </br>
	 * Receives a RDD of trajectories and grid object, split the trajectories 
	 * into segments, and map each segment to partitions (grids) according to
	 * their spatial extent.
	 * 
	 * @param trajectoryRDD
	 * 
	 * @return Return a RDD of key values pairs (PartitionID, PartionOfSegments)
	 */
	public JavaPairRDD<Integer, Partition> partition(JavaRDD<Trajectory> trajectoryRDD, final Grid grid){
		/**
		 * MAP:
		 * Slit each trajectory into segments and map each segment to its
		 * overlapping partition (rectangle). If the segment overlaps more than 
		 * one partition, we split the segment accordingly.
		 */
		 JavaPairRDD<Integer, Segment> mapResultRDD = 
				 trajectoryRDD.flatMapToPair(new PairFlatMapFunction<Trajectory, Integer, Segment>() {
			public Iterable<Tuple2<Integer, Segment>> call(Trajectory trajectory) throws Exception {
				// the map list to return 
				List<Tuple2<Integer, Segment>> mapList = 
						new ArrayList<Tuple2<Integer, Segment>>();
				// read trajectory segments
				for(int i=0; i<trajectory.size()-1; i++){
					Segment s = new Segment(trajectory.get(i), trajectory.get(i+1));
					s.id = i;
					// check for overlapping partitions (grid rectangles)
					for(int j=0; j<grid.size(); j++){
						Rectangle r = grid.get(j);
						// segment is totally covered by this grid rectangle
						if(r.contains(s.x1, s.y1, s.x2, s.y2)){
							// add key/value pair
							mapList.add(new Tuple2<Integer, Segment>(j, s));
							break;
						}
						// check for boundary intersections
						Segment rEdge = r.intersect(s.x1, s.y1, s.x2, s.y2);
						// boundary segment: spans to more than one partition.
						if(rEdge != null){
							// calculate the intersection point and split the segment
							Point pInt = getIntersection(s, rEdge);
							// get time-stamp of p by interpolation
							long t = getTimeStamp(s, pInt.x, pInt.y);
							Segment boundarySeg;
							if(r.contains(s.x1, s.y1)){
								boundarySeg = new Segment(s.x1, s.y1, s.t1, pInt.x, pInt.y, t);
							}
							else{
								boundarySeg = new Segment(pInt.x, pInt.y, t, s.x2, s.y2, s.t2);
							}
							mapList.add(new Tuple2<Integer, Segment>(j, boundarySeg));
						}
					}
				}
				return mapList;
			}
		});
		
		/**
		 * REDUCE:
		 * Using Aggregate by key as reduce function.
		 * Groups (aggregate) segments with same key (partition id) into the same partition.
		 */
		 Partition emptyPartition = new Partition(); // zero value
		 Function2<Partition, Segment, Partition> seqFunc = 
				 new Function2<Partition, Segment, Partition>() {
			public Partition call(Partition partition, Segment s) throws Exception {
				partition.add(s);
				return partition;
			}
		 }; 
		 Function2<Partition, Partition, Partition> combFunc = 
				 new Function2<Partition, Partition, Partition>() {
			public Partition call(Partition partition1, Partition partition2) throws Exception {
				return partition1.merge(partition2);
			}
		 };
		 // call aggregate function (reduce)
		 JavaPairRDD<Integer, Partition> partitionsRDD = 
				 mapResultRDD.aggregateByKey(emptyPartition, seqFunc, combFunc);
		 
		 return partitionsRDD;
	} 
	
	/**
	 * Calculate the intersection point between the given line segments.
	 */
	private static Point getIntersection(Segment s1, Segment s2){
		double x1 = s1.x2 - s1.x1;
		double x2 = s2.x2 - s2.x1;
		double y1 = s1.y2 - s1.y1;
		double y2 = s2.y2 - s2.y1;

		// coeficients
		double a = (s1.x1 * s1.y2) - (s1.y1 * s1.x2);
		double b = (s2.x1 * s2.y2) - (s2.y1 * s2.x2);
		double c = (y1 * x2) - (x1 * y2);
		
		// Intersection
		double x = (a * x2 - b * x1) / c;
		double y = (a * y2 - b * y1) / c;

		return new Point(x, y);
	}
	
	/**
	 * Calculate the time-stamp of the coordinate (x,y) in
	 * the segment s by interpolation of s end points.
	 */
	private static long getTimeStamp(Segment s, double x, double y){
		// delta time
		long dt = s.t2 - s.t1;
		// square length from p1 to p2
		double p1p2Length = (s.x2 - s.x1)*(s.x2 - s.x1) + (s.y2 - s.y1)*(s.y2 - s.y1);
		// square length from p1 to p
		double p1pLength  = (x - s.x1)*(x - s.x1) + (y - s.y1)*(y - s.y1);
		
		double t = s.t1 + dt * (p1pLength / p1p2Length);
		
		return (long)t;
	}
}
