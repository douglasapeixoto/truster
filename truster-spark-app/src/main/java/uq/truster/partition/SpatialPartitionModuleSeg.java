package uq.truster.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import uq.spark.EnvironmentVariables;
import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.Rectangle;
import uq.spatial.STSegment;
import uq.spatial.Segment;
import uq.spatial.Trajectory;

/**
 * TRUSTER Spatial partitioning module.
 * </br>
 * Split trajectories into segments.
 * </br>
 * Split trajectories into segments and map segments 
 * to partitions (grid) according to their spatial extent.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SpatialPartitionModuleSeg implements Serializable, EnvironmentVariables {
	/**
	 * Keep track of trajectory segments across the grid.
	 */
	private TrajectoryTrackTableSeg trackTable = null;
	
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
	public JavaPairRDD<Integer, PartitionSeg> partition(JavaRDD<Trajectory> trajectoryRDD, final Grid grid){
		/**
		 * MAP:
		 * Split each trajectory into segments and map each segment to its
		 * overlapping partition (rectangle). If the segment overlaps more than 
		 * one partition, we split the segment accordingly.
		 */
		

		 JavaPairRDD<Integer, STSegment> mapResultRDD = 
				 trajectoryRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Trajectory>, Integer, STSegment>() {
					public Iterable<Tuple2<Integer, STSegment>> call(
							Iterator<Trajectory> trajectoryItr) throws Exception {

					// the map list to return 
					List<Tuple2<Integer, STSegment>> mapList = 
							new ArrayList<Tuple2<Integer, STSegment>>();
					
					// read each trajectory in this partition
					//int VSI, TPI, prevVSI, prevTPI;
					while(trajectoryItr.hasNext()){
						// current trajectory
						Trajectory trajectory = trajectoryItr.next();
						
						// read trajectory segments
						for(int i=0; i<trajectory.size()-1; i++){
							STSegment s = new STSegment(trajectory.get(i), trajectory.get(i+1));
							s.parentId = trajectory.id;
							// check for overlapping partitions (grid rectangles)
							for(int j=0; j<grid.size(); j++){
								Rectangle r = grid.get(j);
								// segment is totally covered by this grid rectangle
								if(r.contains(s.x1, s.y1, s.x2, s.y2)){
									// add key/value pair (grid,segment)
									mapList.add(new Tuple2<Integer, STSegment>(j, s));
									break;
								}
								// check for boundary intersections
								// boundary segment: spans to more than one partition.
								if(r.intersect(s.x1, s.y1, s.x2, s.y2)){
									// get the intersection edges (1 or 2)
									List<Segment> edgeList = getIntersectEgdes(s, r);
									STSegment boundarySeg;
									// intersect only one edge
									if(edgeList.size() == 1){
										// calculate the intersection point and split the segment
										Point pInt = getIntersection(s, edgeList.get(0));
										// get time-stamp of p by interpolation
										double t = getTimeStamp(s, pInt.x, pInt.y);
										if(r.contains(s.x1, s.y1)){
											boundarySeg = new STSegment(s.x1, s.y1, s.t1, pInt.x, pInt.y, (long)t);
										}
										else{
											boundarySeg = new STSegment(pInt.x, pInt.y, (long)t, s.x2, s.y2, s.t2);
										}
									}
									// intersect two edges
									else{
										// calculate the intersection point and split the segment
										Point pInt1 = getIntersection(s, edgeList.get(0));
										Point pInt2 = getIntersection(s, edgeList.get(1));
										// get time-stamp of p by interpolation
										double t1 = getTimeStamp(s, pInt1.x, pInt1.y);
										double t2 = getTimeStamp(s, pInt2.x, pInt2.y);
										if(t2 > t1){
											boundarySeg = new STSegment(pInt1.x, pInt1.y, (long)t1, pInt2.x, pInt2.y, (long)t2);
										}else{
											boundarySeg = new STSegment(pInt2.x, pInt2.y, (long)t2, pInt1.x, pInt1.y, (long)t1);
										}
									}
									boundarySeg.boundary = true;
									boundarySeg.parentId = trajectory.id;
									// add key/value pair (grid,segment)
									mapList.add(new Tuple2<Integer, STSegment>(j, boundarySeg));
								}
							}
						}
						
					}
					return mapList;
				}
			});

		/**
		 * REDUCE [PARTITION]:
		 * Using Aggregate by key as reduce function.
		 * Groups (aggregate) segments with same key (partition id) into the same partition.
		 */
		 PartitionSeg emptyPartition = new PartitionSeg(); // zero value
		 Function2<PartitionSeg, STSegment, PartitionSeg> seqFunc = 
				 new Function2<PartitionSeg, STSegment, PartitionSeg>() {
			public PartitionSeg call(PartitionSeg partition, STSegment s) throws Exception {
				partition.add(s);
				return partition;
			}
		 }; 
		 Function2<PartitionSeg, PartitionSeg, PartitionSeg> combFunc = 
				 new Function2<PartitionSeg, PartitionSeg, PartitionSeg>() {
			public PartitionSeg call(PartitionSeg partition1, PartitionSeg partition2) throws Exception {
				return partition1.merge(partition2);
			}
		 };
		 // call aggregate function (reduce)
		 JavaPairRDD<Integer, PartitionSeg> partitionsRDD = 
				 mapResultRDD.aggregateByKey(emptyPartition, NUM_PARTITIONS_DATA, seqFunc, combFunc);
		 
		 /**
		 * TTT CONSTRUCTION
		 */
		 trackTable = new TrajectoryTrackTableSeg();
		 trackTable.build(mapResultRDD);
		 
		 return partitionsRDD;
	} 
	
	/**
	 * Return the trajectory track table built during the partitioning phase.
	 */
	public TrajectoryTrackTableSeg getTTT(){
		return trackTable;
	}
	
	/**
	 * Calculate the intersection point between the given line segments.
	 */
	private Point getIntersection(Segment s1, Segment s2){
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
	private double getTimeStamp(STSegment s, double x, double y){
		// delta time
		long dt = s.t2 - s.t1;
		// square length from p1 to p2
		double p1p2Length = (s.x2 - s.x1)*(s.x2 - s.x1) + (s.y2 - s.y1)*(s.y2 - s.y1);
		// square length from p1 to p
		double p1pLength  = (x - s.x1)*(x - s.x1) + (y - s.y1)*(y - s.y1);
		
		double t = s.t1 + dt * (p1pLength / p1p2Length);
		
		return t;
	}
	
	/**
	 * Get the edges of the rectangle r that 
	 * intersect with the segment s.
	 */
	private List<Segment> getIntersectEgdes(Segment s, Rectangle r){
		List<Segment> edgeList = 
				new ArrayList<Segment>();
		// check box LEFT edge
		Segment edge = new Segment(r.min_x, r.min_y, r.min_x, r.max_y);
		if(edge.intersect(s)){
			edgeList.add(edge);
		}
		// check RIGHT edge
		edge = new Segment(r.max_x, r.min_y, r.max_x, r.max_y);
		if(edge.intersect(s)){
			edgeList.add(edge);
		}
		// check TOP edge
		edge = new Segment(r.min_x, r.max_y, r.max_x, r.max_y);
		if(edge.intersect(s)){
			edgeList.add(edge);
		}
		// check BOTTOM edge
		edge = new Segment(r.min_x, r.min_y, r.max_x, r.min_y);
		if(edge.intersect(s)){
			edgeList.add(edge);
		}
		return edgeList;
	}
}
