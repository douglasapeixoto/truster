package uq.truster.partition;

import java.io.Serializable;
import java.util.Collection;
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
import uq.spatial.STSegment;
import uq.spatial.Trajectory;

/**
 * Service responsible to collect trajectories from
 * the index structure. Collect trajectories
 * using the grid index structure and the Trajectory
 * Track Table. Post-process trajectories after collection.
 * 
 * The collection process of trajectories is done in 3 steps:
 * (1) Filter: filter partitions containing the trajectories
 * (2) Collect: collect the segments from the pages
 * (3) Post-processing: Merge segments.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TrajectoryCollectorSeg implements Serializable{
	private JavaPairRDD<Integer, PartitionSeg> partitionsRDD; 
	private TrajectoryTrackTableSeg trackTable;

	/**
	 * Creates a new collector.
	 */
	public TrajectoryCollectorSeg(
			final JavaPairRDD<Integer, PartitionSeg> partitionsRDD, 
			final TrajectoryTrackTableSeg trackTable) {
		this.partitionsRDD = partitionsRDD;
		this.trackTable = trackTable;
	}

	/**
	 * Given a set of partition indexes, and a time interval, 
	 * collect from the RDD the trajectories inside the given 
	 * partitions.
	 * </br>
	 * Use the temporal RTree to prune by time [t0,t1].
	 * </br>
	 * Return whole trajectories (also filter from the RDD other
	 * partitions that might contain trajectories in the given index set.
	 * 
	 * @return Return a distributed dataset (RDD) of trajectories.
	 * If there is no trajectories to collect, then return null. 
	 */
	public JavaRDD<Trajectory> collectTrajectoriesByPartitionIndex(
			final Collection<Integer> indexSet, final long t0, final long t1) {
		// Filter the given partitions
		JavaPairRDD<Integer, PartitionSeg> filteredPartitionsRDD = 
			partitionsRDD.filter(new Function<Tuple2<Integer,PartitionSeg>, Boolean>() {
				public Boolean call(Tuple2<Integer, PartitionSeg> partition) throws Exception {
					return indexSet.contains(partition._1);
				}
			});

		// collection log
		/*System.out.println("Collect Trajectories by Page Index.");
		System.out.println("Total Pages: " + pagesRDD.count());
		System.out.println("Total Pages to Collect: " + filteredPagesRDD.count());
		TOTAL_PAGES_TO_COLLECT += filteredPagesRDD.count();	*/
		
		// check if there is any partition for the given parameters
		// Note: (it might be there is no page in the given time interval for the given grid)
		if(!filteredPartitionsRDD.isEmpty()){			
			// Collect the IDs of the trajectories inside the given partitions.
			// Only those trajectories which the active time satisfy the given time interval
			final List<String> tIdList = 	
					filteredPartitionsRDD.values().flatMap(
							new FlatMapFunction<PartitionSeg, String>() {
					public Iterable<String> call(PartitionSeg partition) throws Exception {
						return partition.getTrajectoryIdSetByTime(t0, t1);
					}
				}).distinct().collect();

			// retrieve from the TTT the indexes of all pages that 
			// contains the trajectories in the list.
			final HashSet<Integer> diffIndexSet = 
					trackTable.collectIndexListByTrajectoryId(tIdList);

			// skip the partitions already retrieved 
			diffIndexSet.removeAll(indexSet);

			// filter the other partitions containing the trajectories (difference set)
			JavaPairRDD<Integer, PartitionSeg> diffPartitionRDD = 
					partitionsRDD.filter(new Function<Tuple2<Integer,PartitionSeg>, Boolean>() {
						public Boolean call(Tuple2<Integer, PartitionSeg> partition) throws Exception {
							return diffIndexSet.contains(partition._1);
						}
					});

			// union the two RDDs (union set)
			filteredPartitionsRDD = filteredPartitionsRDD.union(diffPartitionRDD);

			// map each partition to a list of key value pairs containing 
			// the desired trajectories segments
			JavaPairRDD<String, STSegment> segmentsRDD = 
					filteredPartitionsRDD.values().flatMapToPair(
							new PairFlatMapFunction<PartitionSeg, String, STSegment>() {
					public Iterable<Tuple2<String, STSegment>> call(PartitionSeg partition) throws Exception {
						// iterable list to return
						List<Tuple2<String, STSegment>> list = 
								new LinkedList<Tuple2<String, STSegment>>();
						// prune by time using the tree
						/*List<STSegment> candidatesList = 
								partition.getSegmentsTree().getSegmentsByTime(t0, t1);*/
						for(STSegment s : partition.getSegmentsList()){
							if(tIdList.contains(s.parentId)){
								list.add(new Tuple2<String, STSegment>(s.parentId, s));
							}
						}
						return list;
					}
				});

			// merge segments by key
			Trajectory emptyTrajectory = new Trajectory();
			// aggregate functions
			Function2<Trajectory, STSegment, Trajectory> seqFunc = new Function2<Trajectory, STSegment, Trajectory>() {
				public Trajectory call(Trajectory t, STSegment s) throws Exception {
					t.addSegment(s); 
					t.id = s.parentId;
					return t;
				}
			};
			Function2<Trajectory, Trajectory, Trajectory> combFunc = 
					new Function2<Trajectory, Trajectory, Trajectory>() {
				public Trajectory call(Trajectory t1, Trajectory t2) throws Exception {
					t1.merge(t2);
					return t1;
				}
			};
			
			// aggregate segments by key into a trajectory (unsorted)
			JavaRDD<Trajectory> trajectoryRDD = 
					segmentsRDD.aggregateByKey(emptyTrajectory, seqFunc, combFunc).values();

			// post processing
			trajectoryRDD = postProcess(trajectoryRDD);
			
			// collection log
			/*System.out.println("Total Pages Filtered: " + filteredPagesRDD.count());
			System.out.println("Total Trajectories Filtered (TP+FP): " + idList.size());
			System.out.println("Total Trajectories Collected (TP): " + trajectoryRDD.count());
			TOTAL_TRAJ_FILTERED += idList.size();
			TOTAL_PAGES_FILTERED += filteredPagesRDD.count();
			System.out.println("TOTAIS: ");
			System.out.println("TOTAL TRAJ FILT.: " + TOTAL_TRAJ_FILTERED);
			System.out.println("TOTAL PAGES FILT.: " + TOTAL_PAGES_FILTERED);
			System.out.println("TOTAL PAGES TO COL.: " + TOTAL_PAGES_TO_COLLECT);*/
			
			return trajectoryRDD;
		}
		return null;
	}
	
	/**
	 * Post processing operation.
	 * </br>
	 * Sorts the trajectory points by time stamp
	 * and removes any duplicates form the map phase.
	 * 
	 * @return A post-processed trajectory
	 */
	private Trajectory postProcess(Trajectory t) {
		t.sort();
		int size = t.size();
		for(int i = 0; i < size-1; i++){
			if(t.get(i).equals(t.get(i+1))){
				t.removePoint(i);
				size--;
				--i;
			}
		}
		return t;
	}

	/**
	 * Post processing operation. Done in parallel.
	 * </br>
	 * Sorts the trajectory points by time stamp
	 * and removes any duplicates from the map phase.
	 * 
	 * @return A post-processed RDD of trajectories
	 */
	private JavaRDD<Trajectory> postProcess(
			JavaRDD<Trajectory> trajectoryRDD){
		// map each trajec to its post-process version
		trajectoryRDD = 
			trajectoryRDD.map(new Function<Trajectory, Trajectory>() {
				public Trajectory call(Trajectory t) throws Exception {
					return postProcess(t);
				}
			});
		return trajectoryRDD;
	}
}
