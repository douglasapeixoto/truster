package uq.truster.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uq.fs.DataConverter; 
import uq.spark.EnvironmentVariables; 
import uq.spatial.GeoInterface;
import uq.spatial.Grid;
import uq.spatial.Point;
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
public class SpatialPartitioningModuleSub implements Serializable, EnvironmentVariables, GeoInterface {
	// The Voronoi diagram partition itself.
	private JavaPairRDD<Integer, PartitionSub> partitionsRDD = null;
	
	// Keep track of trajectories across the grid
	private TrajectoryTrackTableSub trackTable = null;
	
	// read-only variable to be cached on each machine. 
	// Contains the grid representation
	private Broadcast<Grid> gridDiagram = null;

	/**
	 * Return the RDD with the grid partitions
	 * created in this service.
	 */
	public JavaPairRDD<Integer, PartitionSub> getPartitionsRDD(){
		return partitionsRDD;
	}
	
	/**
	 * Return the grid diagram built in this service.
	 * Broadcasted diagram.
	 */
	public Broadcast<Grid> getGridDiagram(){
		return gridDiagram;
	}
	
	/**
	 * Return a Trajectory Track Table to keep track of 
	 * trajectories across partitions.
	 */
	public TrajectoryTrackTableSub getTrajectoryTrackTable(){
		return trackTable;
	}
	
	/**
	 * Run data partitioning and indexing.
	 * Build the grid and assign trajectory points to
	 * grid cells, and build the trajectory track table.
	 * Each cell contains a 1D temporal R-Tree of sub-trajectories.
	 */
	public void run(){
		/**
    	 * READ DATA AND BUILD THE GRID
    	 */
    	// read data and convert to trajectories
     	JavaRDD<String> fileRDD = SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
     	fileRDD.persist(STORAGE_LEVEL);   
		
		// convert the input data to a RDD of trajectory objects
		DataConverter converter = new DataConverter(); 
		JavaRDD<Trajectory> trajectoryRDD = 
				converter.mapRawDataToTrajectoryRDD(fileRDD);
     	
		// create a grid and broadcast
		Grid grid = new Grid(
				SIZE_X, SIZE_Y, MIN_X, MIN_Y, MAX_X, MAX_Y);
		gridDiagram = SC.broadcast(grid);
     	
		/**
		 * BUILD THE GRID PARTITIONS AND THE TTT
		 */
     	// assign each sub-trajectory to a grid cell
		JavaPairRDD<Integer, Trajectory> mapResultRDD = 
				partitionMap(trajectoryRDD, getGridDiagram());
		// reduce the partitions
		partitionsRDD = partitionReduce(mapResultRDD);

		// TTT construction
		trackTable = new TrajectoryTrackTableSub();
		trackTable.build(mapResultRDD);		
	}
	
	/**
	 * MAP function:
	 * </br>
	 * Split each trajectory into sub-trajectories and map each sub-trajectory to its
	 * overlapping partition (rectangle).

	 * @returnReturn a RDD of pairs: (GridCell Index, Sub-Trajectory)
	 */
	private JavaPairRDD<Integer, Trajectory> partitionMap(
			final JavaRDD<Trajectory> trajectoryRDD,
			final Broadcast<Grid> gridBroadcast){
		
		// map each trajectories to a list of (GridCell Index, Sub-Trajectory)
		JavaPairRDD<Integer, Trajectory> mapResultRDD = 
				 trajectoryRDD.mapPartitionsToPair(
						 new PairFlatMapFunction<Iterator<Trajectory>, Integer, Trajectory>() {
							 
			// get the grid	
			final Grid grid = gridBroadcast.value();
			
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
		
		return mapResultRDD;
	}
	
	/**
	 * REDUCE function:
	 * </br>
	 * Groups (aggregate) sub-trajectories with same key (partition id)
	 * into the same partition. Using Aggregate by key as reduce function.
	 * 
	 * @returnReturn Ta RDD of pairs: (GridCell Index, Partition)
	 */
	private JavaPairRDD<Integer, PartitionSub>  partitionReduce(
			final JavaPairRDD<Integer, Trajectory> mapResultRDD){
		
		 // aggregates sub-trajectories with same key into the same partition tree
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
		 partitionsRDD = mapResultRDD.aggregateByKey(
				 emptyPartition, NUM_PARTITIONS_RDD, seqFunc, combFunc);
		 
		 return partitionsRDD;
	}
	
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
	/*private JavaPairRDD<Integer, PartitionSub> partition(
			final JavaRDD<Trajectory> trajectoryRDD, 
			final Broadcast<Grid> gridBroadcast){
		/**
		 * MAP:
		 * Split each trajectory into sub-trajectories and map each sub-trajectory to its
		 * overlapping partition (rectangle).
		 */
		/* JavaPairRDD<Integer, Trajectory> mapResultRDD = 
				 trajectoryRDD.mapPartitionsToPair(
						 new PairFlatMapFunction<Iterator<Trajectory>, Integer, Trajectory>() {
 
				// get the grid	
				final Grid grid = gridBroadcast.value();
				
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
		/* PartitionSub emptyPartition = new PartitionSub(); // zero value
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
		/* partitionsRDD = mapResultRDD.aggregateByKey(
				 emptyPartition, NUM_PARTITIONS_DATA, seqFunc, combFunc);
		 
		 /**
		 * TTT CONSTRUCTION
		 */
	/*	 trackTable = new TrajectoryTrackTableSub();
		 trackTable.build(mapResultRDD);
		 
		 return partitionsRDD;
	}*/
}
