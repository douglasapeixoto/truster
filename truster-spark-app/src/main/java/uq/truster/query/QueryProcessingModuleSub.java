package uq.truster.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
 
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import uq.spark.EnvironmentVariables;
import uq.spatial.Circle;
import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.STRectangle;
import uq.spatial.Trajectory;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.TrajectoryDistanceCalculator;
import uq.truster.partition.PartitionSub;
import uq.truster.partition.TrajectoryCollectorSub;
import uq.truster.partition.TrajectoryTrackTableSub;

/**
 * TRUSTER Query processing module.
 * </br>
 * For sub-trajectories partitioning.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class QueryProcessingModuleSub implements Serializable, EnvironmentVariables {
	private Broadcast<Grid> grid;
	private TrajectoryCollectorSub collector;
	private JavaPairRDD<Integer, PartitionSub> partitionsRDD;
	
	// distance measure
	private TrajectoryDistanceCalculator edwp = 
			new EDwPDistanceCalculator();
	// to sort trajectories by distance
	private NeighborComparator<NearNeighbor> nnComparator = 
			new NeighborComparator<NearNeighbor>();
	
	// log info
	/*private static int TOTAL_PARTITIONS_FILTERED = 0;
	private static int TOTAL_TRAJ_FILTERED  = 0;	
	private static int TOTAL_TRAJ_COLLECTED = 0;
	*/
	/**
	 * Set the data partition RDD and the grid used to process queries.
	 */
	public QueryProcessingModuleSub(
			final JavaPairRDD<Integer, PartitionSub> partitionsRDD, 
			final TrajectoryTrackTableSub trackTable,
			final Broadcast<Grid> grid) {
		this.grid = grid;
		this.partitionsRDD = partitionsRDD;
		// set up trajectory collector
		collector = new TrajectoryCollectorSub(partitionsRDD, trackTable);
	}
	
	/**
	 * Given a spatial-temporal query region (spatial-temporal rectangle),
	 * returns from the partition RDD all sub-trajectory (post-processed 
	 * sub-trajectories) that satisfy the query, that is, all sub-trajectories
	 * covered by the query area within the query time interval.
	 * 
	 * @return A list of sub-trajectory that satisfy the query.
	 **/
	public List<Trajectory> processSelectionQuery(
			final STRectangle query){
		System.out.println("\n[TRUSTER] Running Spatial-Temporal Selection..\n");
		
		// get the rectangles in the grid that overlap with the query area
		final HashSet<Integer> idList = grid.value().getOverlappingCells(query);
		
		/**
		 * FILTER STEP:
		 */
		// filter partitions that overlap/cover the query area
		// use Spark filter function
		JavaPairRDD<Integer, PartitionSub> filterRDD = 
			partitionsRDD.filter(new Function<Tuple2<Integer,PartitionSub>, Boolean>() {
				public Boolean call(Tuple2<Integer, PartitionSub> partition) throws Exception {
					return idList.contains(partition._1);
				}
			}).coalesce(COALESCE_NUMBER);

		// collection log
		/*System.out.println("Collect Trajectories by Id.");
		System.out.println("Total Partitions: " + partitionsRDD.count());
		System.out.println("Total Partitions Filtered: " + filterRDD.count());
		//ids of trajectories filtered from the tree inside the partitions (filter step)
		int numFilteredTrajectories = 
			filterRDD.flatMap(new FlatMapFunction<Tuple2<Integer,PartitionSub>, String>() {
				public Iterable<String> call(Tuple2<Integer, PartitionSub> partition)
						throws Exception {
					HashSet<String> idSet = new HashSet<String>();
					List<Trajectory> candidatesList =
							partition._2.getSubTrajectoryTree().getTrajectoriesByTime(query.t1, query.t2);
					for(Trajectory t : candidatesList){
						idSet.add(t.id);
					}
					return idSet;
				}
			}).distinct().collect().size();
		//get the number of trajectories filtered (TP+FP)		
		System.out.println("Total Trajectories Filtered (TP+FP): " + numFilteredTrajectories);
		TOTAL_TRAJ_FILTERED += numFilteredTrajectories;	
		TOTAL_PARTITIONS_FILTERED += filterRDD.count();
		*/		
		
		/**
		 * REFINEMENT STEP:
		 */
		// map each partition to a list of sub-trajectories that satisfy the query
		JavaPairRDD<String, Trajectory> refinedSubTrajectoriesRDD = 
			filterRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,PartitionSub>, String, Trajectory>() {
				public Iterable<Tuple2<String, Trajectory>> call(
						Tuple2<Integer, PartitionSub> partition) throws Exception {
					List<Tuple2<String, Trajectory>> tList = 
							new ArrayList<Tuple2<String, Trajectory>>();
					List<Trajectory> candidatesList =
							partition._2.getSubTrajectoryTree().getTrajectoriesByTime(query.t1, query.t2);
					for(Trajectory t : candidatesList){
						// refine: if at least one point satisfy the query, 
						// then add the sub-trajectory to the result list
						if(t.timeIni() > query.t2 || t.timeEnd() < query.t1){continue;}
						for(Point p : t.getPointsList()){
							if(query.contains(p)){
								tList.add(new Tuple2<String, Trajectory>(t.id, t));
								break;
							}
						}
					}
					return tList;
				}
			});
			
		// get the number of trajectories that satisfy the query (TP)
		/*int totalTrajectories = 
			refinedSubTrajectoriesRDD.values().map(new Function<Trajectory, String>() {
				public String call(Trajectory sub) throws Exception {
					return sub.id;
				}
			}).distinct().collect().size();
		// collection log
		System.out.println("Total Trajectories Collected (TP): " + totalTrajectories);
		TOTAL_TRAJ_COLLECTED += totalTrajectories;
		System.out.println("TOTAIS: ");
		System.out.println("TOTAL PARTITIONS FILT.: " + TOTAL_PARTITIONS_FILTERED);
		System.out.println("TOTAL TRAJ FILT.:  " + TOTAL_TRAJ_FILTERED);
		System.out.println("TOTAL TRAJ COL.:   " + TOTAL_TRAJ_COLLECTED);
		System.out.println();			
		*/	
		
		/**
		 * POST-PROCESSING:
		 */
		List<Trajectory> resultList = postProcess(refinedSubTrajectoriesRDD);
		
		return resultList;
	}
	
	/**
	 * NN Query:
	 * Given a query trajectory Q and a time interval t0 to t1,
	 * return the Nearest Neighbor (Most Similar Trajectory) 
	 * from Q, within the interval [t0,t1]. 
	 */
	public NearNeighbor processNNQuery(
			final Trajectory q, 
			final long t0, final long t1){
		List<NearNeighbor> result = 
				processKNNQuery(q, t0, t1, 1);
		NearNeighbor nn = new NearNeighbor();
		if(result!=null && !result.isEmpty()){
			nn = result.get(0);
		}
		return nn;
	}

	/**
	 * Given a query trajectory Q, a time interval [t0,t1], and a integer K,
	 * returns from the partition RDD K most nearest neighbor of Q within
	 * the given time interval [t0,t1].
	 * 
	 * @return A list of sub-trajectory that satisfy the query.
	 **/
	public List<NearNeighbor> processKNNQuery(
			final Trajectory query, final long t0, final long t1, final int k){
		System.out.println("\n[TRUSTER] Running " + k + "-NN Query..\n");
		
		/**
		 * FIRST FILTER:
		 */
		// check the grid cells that overlaps with the query 
		final HashSet<Integer> gridIdSet = 
				getOverlappingRectangles(query);

		// collect the trajectories inside those grid partitions (whole trajectories)
		JavaRDD<Trajectory> trajectoryRDD = 
				collector.collectTrajectoriesByPartitionIndex(gridIdSet, t0, t1);

		/**
		 * FIRST REFINEMENT:
		 * Get the kNN inside the partitions containing the query trajectory
		 */
		// get first candidates (trajectories in the grids containing Q), 
		// map each trajectory to a NN object
		List<NearNeighbor> candidatesList = new LinkedList<NearNeighbor>();
		candidatesList = getCandidatesNN(trajectoryRDD, candidatesList, query, t0, t1);

		/**
		 * SECOND FILTER:
		 */
		// get the k-th-NN returned
		Trajectory knn;
		if(candidatesList.size() >= k){
			knn = candidatesList.get(k-1);
		} else if(candidatesList.size() > 0){
			knn = candidatesList.get(candidatesList.size()-1);
		} else{ // no candidates to return (need to extend the search area)
			return candidatesList;
		}
		
		// get the circle made from the centroids
		Circle c = getCentroidCircle(query, knn);

		// check the grid rectangles that overlaps with the query,
		// except those already retrieved
		final HashSet<Integer> extendGridIdSet = 
				grid.value().getOverlappingCells(c.mbr());
		/*final HashSet<Integer> extendGridIdSet = 
				grid.value().getOverlappingCells(c);*/
		extendGridIdSet.removeAll(gridIdSet);

		/**
		 * SECOND REFINEMENT:
		 */
		// if there are other grids to check
		if(extendGridIdSet.size() > 0){
			// collect the new trajectories
			JavaRDD<Trajectory> extendTrajectoryRDD = 
				collector.collectTrajectoriesByPartitionIndex(extendGridIdSet, t0, t1);

			// refine and update the candidates list
			candidatesList = getCandidatesNN(extendTrajectoryRDD, candidatesList, query, t0, t1);
		}
		
		// collect result
		if(candidatesList.size() >= k){
			return candidatesList.subList(0, k);
		}
		return candidatesList;
	}
	
	/**
	 * Given two trajectories t1 and t2, calculate the circle
	 * composed of the centroid of t1 as center, and the distance
	 * between t1's centroid and t2's centroid as radius.
	 */
	private Circle getCentroidCircle(Trajectory t1, Trajectory t2) {
		Point t1_centroid = t1.centroid();
		Point t2_centroid = t2.centroid();
		double dist = t1_centroid.dist(t2_centroid);
		return new Circle(t1_centroid, dist);
	}
	
	/**
	 * Return the positions (index) of the rectangles in the grid that
	 * overlaps with the given trajectory, that is, rectangles that 
	 * contain or intersect any of the trajectory's segments.
	 */
	private HashSet<Integer> getOverlappingRectangles(Trajectory t) {
		HashSet<Integer> posSet = new HashSet<Integer>();
		for(Point p : t.getPointsList()){
			/*Point p1 = t.get(i);
			Point p2 = t.get(i+1);
			Segment s = new Segment(p1.x, p1.y, p2.x, p2.y);*/
			posSet.add(grid.value().getOverlappingCell(p));
		}
		return posSet;
	}
	
	/**
	 * Check the trajectories time-stamp and 
	 * calculate the distance between every trajectory in the RDD to
	 * the query trajectory, return a sorted list of NN by distance.
	 * </br>
	 * Calculate the NN object only for the new trajectories (i.e.  
	 * trajectories not contained in current list).
	 * 
	 * @return Return the updated current NN list.
	 */
	private List<NearNeighbor> getCandidatesNN(
			final JavaRDD<Trajectory> candidateRDD, 
			final List<NearNeighbor> currentList,
			final Trajectory q,
			final long t0, final long t1){
		
		List<NearNeighbor> nnCandidatesList = 
			// filter out new trajectories, and refine time
			candidateRDD.filter(new Function<Trajectory, Boolean>() {
				public Boolean call(Trajectory t) throws Exception {
					if(t.timeIni() > t1 || t.timeEnd() < t0){
						return false;
					}
					return (!currentList.contains(t));
				}
			// map each new trajectory in the candidates list to a NN
			}).map(new Function<Trajectory, NearNeighbor>() {
				public NearNeighbor call(Trajectory t) throws Exception {
					NearNeighbor nn = new NearNeighbor(t);
					nn.distance = edwp.getDistance(q, t);
					return nn;
				}
			}).collect();
		// add new candidates
		currentList.addAll(nnCandidatesList);
		// sort by distance to Q
		Collections.sort(currentList, nnComparator);
		
		return currentList;
	}
	
	/**
	 * The post-processing phase of the selection query (not whole).
	 * </br>
	 * Aggregate sub-trajectories by key and post-process.
	 */
	private List<Trajectory> postProcess(
			final JavaPairRDD<String, Trajectory> subTrajectoryRDD){
		// an empty bag of sub-trajectories to start aggregating
		SubTrajectoryBag emptyObj = new SubTrajectoryBag();
		// group sub-trajectories belonging to the same parent trajectory
		Function2<SubTrajectoryBag, Trajectory, SubTrajectoryBag> seqFunc = 
				new Function2<SubTrajectoryBag, Trajectory, SubTrajectoryBag>() {
			public SubTrajectoryBag call(SubTrajectoryBag obj, Trajectory t) throws Exception {
				obj.add(t);
				return obj;
			}
		};
		Function2<SubTrajectoryBag, SubTrajectoryBag, SubTrajectoryBag> combFunc = 
				new Function2<SubTrajectoryBag, SubTrajectoryBag, SubTrajectoryBag>() {
			public SubTrajectoryBag call(SubTrajectoryBag obj1, SubTrajectoryBag obj2) throws Exception {
				return obj1.merge(obj2);
			}
		};
		// aggregate the sub-trajectories by key, and post-process
		List<Trajectory> selectList =
			subTrajectoryRDD.aggregateByKey(emptyObj, seqFunc, combFunc)
				.values().flatMap(new FlatMapFunction<SubTrajectoryBag, Trajectory>() {
					public Iterable<Trajectory> call(SubTrajectoryBag bag) throws Exception {
						// post-process and return
						return bag.postProcess();
					}
			}).collect();

		return selectList;			
	}
}
