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
import uq.spatial.Trajectory;

/**
 * TRUSTER Spatial partitioning module.
 * </br>
 * Split trajectories into sub-trajectories.
 * </br>
 * Split trajectories into sub-trajectories and map
 * sub-trajectories to partitions  (grid) according
 * to their spatial extent.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SpatialPartitionModuleSub implements Serializable, EnvironmentVariables {
	/**
	 * Keep track of trajectory segments across the grid.
	 */
	private TrajectoryTrackTableSub trackTable = null;
	
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
	public JavaPairRDD<Integer, PartitionSub> partition(JavaRDD<Trajectory> trajectoryRDD, final Grid grid){
		/**
		 * MAP:
		 * Split each trajectory into sub-trajectories and map each sub-trajectory to its
		 * overlapping partition (rectangle).
		 */
		 JavaPairRDD<Integer, Trajectory> mapResultRDD = 
				 trajectoryRDD.mapPartitionsToPair(
						 new PairFlatMapFunction<Iterator<Trajectory>, Integer, Trajectory>() {
				public Iterable<Tuple2<Integer, Trajectory>> call(
							Iterator<Trajectory> trajectoryItr) throws Exception {
				
				// the result pairs (PageIndex, Sub-trajectory)
				List<Tuple2<Integer, Trajectory>> resultPairs = 
						  new ArrayList<Tuple2<Integer,Trajectory>>();
				
				// read each trajectory in this partition
				//int VSI, TPI, prevVSI, prevTPI;
				while(trajectoryItr.hasNext()){
					// current trajectory
					Trajectory trajectory = trajectoryItr.next();

					// info of the previous point
					Point prev = null;
					int prevIndex = 0;
					
					// an empty sub-trajectory
					String id = trajectory.id;
					Trajectory sub = new Trajectory(id);
					
					// split the trajectory into sub-trajectories
					// for each page it intersects with
					for(Point point : trajectory.getPointsList()){
						// current point index
						// find the cell in the grid containing this point
						int index = grid.getOverlappingCell(point);
						point.gridId = index;
						
						// check for boundary objects
						if(prev == null){
							sub.addPoint(point);
						} else if(index == prevIndex){
							sub.addPoint(point);
						} 
						// boundary segment
						else {
							// the current sub-trajectory also receives this boundary point
							sub.addPoint(point);

							// add pair <(VSI,TPI), Sub-Trajectory>
							resultPairs.add(new Tuple2<Integer, Trajectory>(index, sub));
							
							// new sub-trajectory for this boundary segment
							sub = new Trajectory(id);
							sub.addPoint(prev);
							sub.addPoint(point);
						}
						prev = point;
						prevIndex = index;
					}
					// add the pair <PageIndex, Page> for the last sub-trajectory read
					resultPairs.add(new Tuple2<Integer, Trajectory>(prevIndex, sub));
				}
				
				// the iterable map list
				return resultPairs;
			}
		});

		/**
		 * REDUCE [PARTITION]:
		 * Using Aggregate by key as reduce function.
		 * Groups (aggregate) sub-trajectories with same key (partition id) into the same partition.
		 */
		 PartitionSub emptyPartition = new PartitionSub(); // zero value
		 Function2<PartitionSub, Trajectory, PartitionSub> seqFunc = 
				 new Function2<PartitionSub, Trajectory, PartitionSub>() {
			public PartitionSub call(PartitionSub partition, Trajectory t) throws Exception {
				partition.add(t);
				return partition;
			}
		 }; 
		 Function2<PartitionSub, PartitionSub, PartitionSub> combFunc = 
				 new Function2<PartitionSub, PartitionSub, PartitionSub>() {
			public PartitionSub call(PartitionSub partition1, PartitionSub partition2) throws Exception {
				return partition1.merge(partition2);
			}
		 };
		 // call aggregate function (reduce)
		 JavaPairRDD<Integer, PartitionSub> partitionsRDD = 
				 mapResultRDD.aggregateByKey(emptyPartition, NUM_PARTITIONS_DATA, seqFunc, combFunc);
		 
		 /**
		 * TTT CONSTRUCTION
		 */
		 trackTable = new TrajectoryTrackTableSub();
		 trackTable.build(mapResultRDD);
		 
		 return partitionsRDD;
	} 
	
	/**
	 * Return the trajectory track table built during the partitioning phase.
	 */
	public TrajectoryTrackTableSub getTTT(){
		return trackTable;
	}
}
