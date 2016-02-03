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

import scala.Tuple2;
import uq.spatial.Circle;
import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.STRectangle;
import uq.spatial.STSegment;
import uq.spatial.Segment;
import uq.spatial.Trajectory;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.EuclideanDistanceCalculator;
import uq.spatial.distance.PointDistanceCalculator;
import uq.spatial.distance.TrajectoryDistanceCalculator;
import uq.truster.partition.PartitionSeg;
import uq.truster.partition.TrajectoryCollectorSeg;
import uq.truster.partition.TrajectoryTrackTableSeg;

/**
 * TRUSTER Query processing module.
 * </br>
 * For segments partitioning.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class QueryProcessingModuleSeg implements Serializable {
	private JavaPairRDD<Integer, PartitionSeg> partitionsRDD; 
	private Grid grid;
	private TrajectoryCollectorSeg collector;
	
	// distance measure
	private TrajectoryDistanceCalculator edwp = 
			new EDwPDistanceCalculator();
	private PointDistanceCalculator euclid = 
			new EuclideanDistanceCalculator();
	// to sort trajectories by distance
	private NeighborComparator<NearNeighbor> nnComparator = 
			new NeighborComparator<NearNeighbor>();
	
	/**
	 * Set the data partition RDD and the grid used to process queries.
	 */
	public QueryProcessingModuleSeg(
			final JavaPairRDD<Integer, PartitionSeg> partitionsRDD, 
			final TrajectoryTrackTableSeg trackTable,
			final Grid grid) {
		this.partitionsRDD = partitionsRDD;
		this.grid = grid;
		// set up trajectory collector
		collector = new TrajectoryCollectorSeg(partitionsRDD, trackTable);
	}

	/**
	 * Given a spatial-temporal query region (spatial-temporal rectangle),
	 * returns from the partition RDD all sub-trajectory (post-processed segments) 
	 * that satisfy the query, that is, all segments covered by the query 
	 * area within the query time interval.
	 * 
	 * @return A list of sub-trajectory that satisfy the query.
	 **/
	public List<Trajectory> processSelectionQuery(
			final STRectangle query){
		System.out.println("\n[TRUSTER] Running Spatial-Temporal Selection..\n");
		
		// get the rectangles in the grid that overlap with the query area
		final List<Integer> idList = grid.getOverlappingRectangles(query);
		
		/**
		 * FILTER STEP:
		 */
		// filter partitions that overlap/cover the query area
		// use Spark filter function
		JavaPairRDD<Integer, PartitionSeg> filterRDD = 
			partitionsRDD.filter(new Function<Tuple2<Integer,PartitionSeg>, Boolean>() {
				public Boolean call(Tuple2<Integer, PartitionSeg> partition) throws Exception {
					return idList.contains(partition._1); 
				}
			});
		
		/**
		 * REFINEMENT STEP:
		 */
		// map each partition to a list of segments that satisfy the query
		JavaPairRDD<String, STSegment> refineSegmentsRDD = 
			filterRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,PartitionSeg>, String, STSegment>() {
				// refinement function
				public Iterable<Tuple2<String, STSegment>> call(Tuple2<Integer, PartitionSeg> partition) throws Exception {
					List<Tuple2<String, STSegment>> segmentsList = 
							new ArrayList<Tuple2<String, STSegment>>();
					List<STSegment> candidatesList =
							partition._2.getSegmentsTree().getSegmentsByTime(query.t1, query.t2);
					for(STSegment s : candidatesList){
						// refine: if at least of end point satisfy the query, 
						// then add the segment to the result list
						if(query.contains(s)){
							if((s.t1 >= query.t1 && s.t1 <= query.t2) ||
							   (s.t2 >= query.t1 && s.t2 <= query.t2)){
								segmentsList.add(new Tuple2<String, STSegment>(s.parentId, s));
							}
						}
					}
					return segmentsList;
				}
			});
		
		/**
		 * POST-PROCESSING:
		 */
		List<Trajectory> resultList = postProcess(refineSegmentsRDD);
		
		return resultList;
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
		// check the grid rectangles that overlaps with the query 
		final HashSet<Integer> gridIdSet = 
				getOverlappingRectangles(query);
System.out.println("Num. Overlap. Grids with Query (1): " + gridIdSet.size());		
		// collect the trajectories inside those grid partitions (whole trajectories)
		JavaRDD<Trajectory> trajectoryRDD = 
				collector.collectTrajectoriesByPartitionIndex(gridIdSet, t0, t1);
System.out.println("Num. Trajectories Filtered (1): " + trajectoryRDD.count());
		/**
		 * FIRST REFINEMENT:
		 * Get the kNN inside the partitions containing the query trajectory
		 */
		// get first candidates (trajectories in the grids containing Q), 
		// map each trajectory to a NN object
		List<NearNeighbor> candidatesList = new LinkedList<NearNeighbor>();
		candidatesList = getCandidatesNN(trajectoryRDD, candidatesList, query, t0, t1);
System.out.println("Candidate List Size (1): " + candidatesList.size());
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
		
		// get the MBR of the circle composed by the farthest distance 
		// and farthest point
		Circle c = getFarthestPointCircle(query, knn);
System.out.println("Query Circle: ");
c.toString();
		// check the grid rectangles that overlaps with the query,
		// except those already retrieved
		final List<Integer> extendGridIdSet = 
				grid.getOverlappingRectangles(c.mbr());
		extendGridIdSet.removeAll(gridIdSet);
System.out.println("Num. Overlap. Grids with Circle MBR (2): " + extendGridIdSet.size());	
		/**
		 * SECOND REFINEMENT:
		 */
		// if there are other grids to check
		if(extendGridIdSet.size() > 0){
			// collect the new trajectories
			JavaRDD<Trajectory> extendTrajectoryRDD = 
				collector.collectTrajectoriesByPartitionIndex(extendGridIdSet, t0, t1);
System.out.println("Num. Trajectories Filtered (2): " + extendTrajectoryRDD.count());
			// refine and update the candidates list
			candidatesList = getCandidatesNN(extendTrajectoryRDD, candidatesList, query, t0, t1);
System.out.println("Candidate List Size (2): " + candidatesList.size());
		}
		
		// collect result
		if(candidatesList.size() >= k){
			return candidatesList.subList(0, k);
		}
		return candidatesList;
	}
	
	/**
	 * Given two trajectories t1 and t2, calculate the circle composed of 
	 * the centroid of t1 as center, and the farthest distance
	 * between t1 and t2 sample points as radius.
	 */
	private Circle getFarthestPointCircle(Trajectory t1, Trajectory t2) {
		double farthestDist = 0;
		double dist;
		double x = 0, y = 0;
		for(Point p1 : t1.getPointsList()){
			for(Point p2 : t2.getPointsList()){
				dist = euclid.getDistance(p1, p2);
				if(dist > farthestDist){
					farthestDist = dist;
				}
			}
			x += p1.x;
			y += p1.y;
		}
		int size = t1.size();
		Point centroid = new Point(x/size, y/size);
		return new Circle(centroid, farthestDist);
	}

	/**
	 * The post-processing phase of the selection query.
	 * </br>
	 * Aggregate segments by key and post-process.
	 */
	private List<Trajectory> postProcess(
			final JavaPairRDD<String, STSegment> subTrajectoryRDD){
		// an empty list of segments to start aggregating
		SegmentBag emptyObj = new SegmentBag();
		// group segments belonging to the same parent trajectory
		Function2<SegmentBag, STSegment, SegmentBag> seqFunc = 
				new Function2<SegmentBag, STSegment, SegmentBag>() {
			public SegmentBag call(SegmentBag bag, STSegment segment) throws Exception {
				bag.add(segment);
				return bag;
			}
		};
		Function2<SegmentBag, SegmentBag, SegmentBag> combFunc = 
				new Function2<SegmentBag, SegmentBag, SegmentBag>() {
			public SegmentBag call(SegmentBag bag1, SegmentBag bag2) throws Exception {
				return bag1.merge(bag2);
			}
		};
		// aggregate segments by key, and post-process
		List<Trajectory> selectList =
			subTrajectoryRDD.aggregateByKey(emptyObj, seqFunc, combFunc)
				.values().flatMap(new FlatMapFunction<SegmentBag, Trajectory>() {
					public Iterable<Trajectory> call(SegmentBag bag) throws Exception {
						// post-process and return
						return bag.postProcess();
					}
			}).collect();

		return selectList;			
	}
	
	/**
	 * Return the positions (index) of the rectangles in the grid that
	 * overlaps with the given trajectory, that is, rectangles that 
	 * contain or intersect any of the trajectory's segments.
	 */
	private HashSet<Integer> getOverlappingRectangles(Trajectory t) {
		HashSet<Integer> posSet = new HashSet<Integer>();
		for(int i=0; i<t.size()-1; i++){
			Point p1 = t.get(i);
			Point p2 = t.get(i+1);
			Segment s = new Segment(p1.x, p1.y, p2.x, p2.y);
			posSet.addAll(grid.getOverlappingRectangles(s));
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
}
