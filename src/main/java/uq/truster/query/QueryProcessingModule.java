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
import uq.spatial.Rectangle;
import uq.spatial.STRectangle;
import uq.spatial.STSegment;
import uq.spatial.Segment;
import uq.spatial.Trajectory;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.EuclideanDistanceCalculator;
import uq.truster.partition.Partition;
import uq.truster.partition.TrajectoryCollector;
import uq.truster.partition.TrajectoryTrackTable;

/**
 * TRUSTER Query processing module.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class QueryProcessingModule implements Serializable {
	private JavaPairRDD<Integer, Partition> partitionsRDD; 
	private Grid grid;
	private TrajectoryCollector collector;
	
	// distance measure
	private EDwPDistanceCalculator edwp = 
			new EDwPDistanceCalculator();
	private EuclideanDistanceCalculator euclid = 
			new EuclideanDistanceCalculator();
	// to sort trajectories by distance
	private NeighborComparator<NearNeighbor> nnComparator = 
			new NeighborComparator<NearNeighbor>();
	
	/**
	 * Set the data partition RDD and the grid used to process queries.
	 */
	public QueryProcessingModule(
			final JavaPairRDD<Integer, Partition> partitionsRDD, 
			final TrajectoryTrackTable trackTable,
			final Grid grid) {
		this.partitionsRDD = partitionsRDD;
		this.grid = grid;
		// set up trajectory collector
		collector = new TrajectoryCollector(partitionsRDD, trackTable);
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
		
		// get the rectangles in the grid that overlap with the query area
		final List<Integer> idList = grid.getOverlappingRectangles(query);
		
		/**
		 * FILTER STEP:
		 */
		// filter partitions that overlap/cover the query area
		// use Spark filter function
		JavaPairRDD<Integer, Partition> filterRDD = 
			partitionsRDD.filter(new Function<Tuple2<Integer,Partition>, Boolean>() {
				@Override
				public Boolean call(Tuple2<Integer, Partition> partition) throws Exception {
					return idList.contains(partition._1); 
				}
			});
		
		/**
		 * REFINEMENT STEP:
		 */
		//	JavaRDD<STSegment> refineRDD = 
		JavaPairRDD<String, STSegment> refineSegmentsRDD = 
			filterRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Partition>, String, STSegment>() {
				// refinement function
				@Override
				public Iterable<Tuple2<String, STSegment>> call(Tuple2<Integer, Partition> partition) throws Exception {
					List<Tuple2<String, STSegment>> segmentsList = 
							new ArrayList<Tuple2<String, STSegment>>();
					// refine
					List<STSegment> candidatesList =
							partition._2.getSegmentsTree().getSegmentsByTime(query.t1, query.t2);
					for(STSegment s : candidatesList){
						if(query.contains(s.x1, s.y1, s.x2, s.y2) &&
							s.t1>=query.t1 && s.t2<=query.t2){
							segmentsList.add(new Tuple2<String, STSegment>(s.parentId, s));
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
	 * Given a spatial-temporal query region (spatial-temporal rectangle),
	 * returns from the partition RDD all sub-trajectory (post-processed segments) 
	 * that satisfy the query, that is, all segments covered by the query 
	 * area within the query time interval.
	 * 
	 * @return A list of sub-trajectory that satisfy the query.
	 **/
	public List<NearNeighbor> processKNNQuery(
			final Trajectory query, final long t0, final long t1, final int k){
		/**
		 * FIRST FILTER:
		 */
		// check the grid rectangles that overlaps with the query 
		final HashSet<Integer> gridIdSet = 
				getOverlappingRectangles(query);
		
		// collect the trajectories inside those grid partitions (whole trajectories)
		JavaRDD<Trajectory> trajectoryRDD = 
				collector.collectTrajectoriesByPartitionIndex(gridIdSet);

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
		// get the farthest distance to the knn trajectory
		Point fartherPoint = new Point();
		Trajectory knn;
		if(candidatesList.size() >= k){
			knn = candidatesList.get(k-1);
		} else{
			knn = candidatesList.get(candidatesList.size()-1);
		}
		double farthestDistance = getFarthestDistance(query, knn, fartherPoint);
		
		// get the MBR of the circle composed by the farthest distance 
		// and farthest point
		Circle c = new Circle(fartherPoint, farthestDistance);
		Rectangle queryRegion = c.mbr();

		// check the grid rectangles that overlaps with the query,
		// except those already retrieved
		final List<Integer> extendGridIdSet = 
				grid.getOverlappingRectangles(queryRegion);
		extendGridIdSet.removeAll(gridIdSet);

		// collect the new trajectories
		JavaRDD<Trajectory> extendTrajectoryRDD = 
				collector.collectTrajectoriesByPartitionIndex(extendGridIdSet);

		/**
		 * SECOND REFINEMENT:
		 */
		// update the candidates list
		candidatesList = getCandidatesNN(extendTrajectoryRDD, candidatesList, query, t0, t1);
				
		if(candidatesList.size() >= k){
			return candidatesList.subList(0, k);
		}
		return candidatesList;
	}
	
	/**
	 * Given two trajectories t and q, return the farthest distance
	 * between these trajectories sample points.
	 * </br>
	 * Set the points with farthest distance of t in farthest_t.
	 * 
	 * @return
	 */
	private double getFarthestDistance(
			Trajectory t, Trajectory q,
			Point farthest_t) {
		double farthestDist = 0;
		double dist;
		for(Point pt : t.getPointsList()){
			for(Point pq : q.getPointsList()){
				dist = euclid.getDistance(pt, pq);
				if(dist > farthestDist){
					farthestDist = dist;
					farthest_t = pt;
				}
			}
		}
		return farthestDist;
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
			public SegmentBag call(SegmentBag obj, STSegment t) throws Exception {
				obj.add(t);
				return obj;
			}
		};
		Function2<SegmentBag, SegmentBag, SegmentBag> combFunc = 
				new Function2<SegmentBag, SegmentBag, SegmentBag>() {
			public SegmentBag call(SegmentBag obj1, SegmentBag obj2) throws Exception {
				return obj1.merge(obj2);
			}
		};
		// aggregate the sub-trajectories by key, and post-process
		List<Trajectory> selectList =
			subTrajectoryRDD.aggregateByKey(emptyObj, seqFunc, combFunc)
				.values().flatMap(new FlatMapFunction<SegmentBag, Trajectory>() {
					public Iterable<Trajectory> call(SegmentBag obj) throws Exception {
						// post-process and return
						return obj.postProcess();
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
