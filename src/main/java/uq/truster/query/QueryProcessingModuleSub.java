package uq.truster.query;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import uq.spatial.Circle;
import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.Segment;
import uq.spatial.Trajectory;
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.EuclideanDistanceCalculator;
import uq.spatial.distance.PointDistanceCalculator;
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
public class QueryProcessingModuleSub implements Serializable {
	private Grid grid;
	private TrajectoryCollectorSub collector;
	
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
	public QueryProcessingModuleSub(
			final JavaPairRDD<Integer, PartitionSub> partitionsRDD, 
			final TrajectoryTrackTableSub trackTable,
			final Grid grid) {
		this.grid = grid;
		// set up trajectory collector
		collector = new TrajectoryCollectorSub(partitionsRDD, trackTable);
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
