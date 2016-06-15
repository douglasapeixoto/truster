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
import uq.spark.EnvironmentVariables;
import uq.spatial.Trajectory;

/**
 * Service responsible to collect trajectories from
 * the index structure. Collect trajectories
 * using the grid index structure and the Trajectory
 * Track Table. Post-process trajectories after collection.
 * 
 * The collection process of trajectories is done in 3 steps:
 * (1) Filter: filter partitions containing the trajectories
 * (2) Collect: collect the sub-trajectories from the partitions
 * (3) Post-processing: Merge sub-trajectories.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TrajectoryCollectorSub implements Serializable, EnvironmentVariables {
	private JavaPairRDD<Integer, PartitionSub> partitionsRDD; 
	private TrajectoryTrackTableSub trackTable;
	
	// log info
	/*private static int TOTAL_TRAJ_FILTERED = 0;
	private static int TOTAL_PARTITIONS_FILTERED = 0;
	private static int TOTAL_PARTITIONS_TO_COLLECT = 0;*/
	
	/**
	 * Creates a new collector.
	 */
	public TrajectoryCollectorSub(
			final JavaPairRDD<Integer, PartitionSub> partitionsRDD, 
			final TrajectoryTrackTableSub trackTable) {
		this.partitionsRDD = partitionsRDD;
		this.trackTable = trackTable;
	}

	/**
	 * Given a set of partition indexes, and a time interval, 
	 * collect from the RDD the trajectories inside the given 
	 * partitions.
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
		JavaPairRDD<Integer, PartitionSub> filteredPartitionsRDD = 
			partitionsRDD.filter(new Function<Tuple2<Integer,PartitionSub>, Boolean>() {
				public Boolean call(Tuple2<Integer, PartitionSub> partition) throws Exception {
					return indexSet.contains(partition._1);
				}
			}).coalesce(COALESCE_NUMBER);

		// collection log
		/*System.out.println("Collect Trajectories by Partition Index.");
		System.out.println("Total Partitions: " + partitionsRDD.count());
		System.out.println("Total Partitions to Collect: " + filteredPartitionsRDD.count());
		TOTAL_PARTITIONS_TO_COLLECT += filteredPartitionsRDD.count();*/
		
		// check if there is any partition for the given parameters
		// Note: (it might be there is no page in the given time interval for the given grid)
		if(!filteredPartitionsRDD.isEmpty()){			
			// Collect the IDs of the trajectories inside the given partitions.
			// Only those trajectories which the active time satisfy the given time interval
			final List<String> tIdList = 	
					filteredPartitionsRDD.values().flatMap(
							new FlatMapFunction<PartitionSub, String>() {
					public Iterable<String> call(PartitionSub partition) throws Exception {
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
			JavaPairRDD<Integer, PartitionSub> diffPartitionRDD = 
					partitionsRDD.filter(new Function<Tuple2<Integer,PartitionSub>, Boolean>() {
						public Boolean call(Tuple2<Integer, PartitionSub> partition) throws Exception {
							return diffIndexSet.contains(partition._1);
						}
					}).coalesce(COALESCE_NUMBER);

			// union the two RDDs (union set)
			filteredPartitionsRDD = filteredPartitionsRDD.union(diffPartitionRDD);

			// map each partition to a list of key value pairs containing 
			// the desired sub-trajectories
			JavaRDD<Trajectory> trajectoryRDD =
					filteredPartitionsRDD.values().flatMapToPair(
							new PairFlatMapFunction<PartitionSub, String, Trajectory>() {
					public Iterable<Tuple2<String, Trajectory>> call(PartitionSub partition) throws Exception {
						// iterable list to return
						List<Tuple2<String, Trajectory>> list = 
								new LinkedList<Tuple2<String, Trajectory>>();
						// prune by time using the tree
						// TODO: check this part
						/*List<Trajectory> candidatesList = 
								partition.getSubTrajectoryTree().getTrajectoriesByTime(t0, t1);*/
						for(Trajectory sub : /*candidatesList*/ partition.getSubTrajectoryList()){
							if(tIdList.contains(sub.id)){
								list.add(new Tuple2<String, Trajectory>(sub.id, sub));
							}
						}
						return list;
					}
					// merge trajectories by key
				}).reduceByKey(new Function2<Trajectory, Trajectory, Trajectory>() {
					public Trajectory call(Trajectory sub1, Trajectory sub2) throws Exception {
						sub1.merge(sub2);
						return sub1;
					}
				}).values();
			
			// post processing
			trajectoryRDD = postProcess(trajectoryRDD);
			
			// collection log
			/*System.out.println("Total Partitions Filtered: " + filteredPartitionsRDD.count());
			System.out.println("Total Trajectories Filtered (TP+FP): " + tIdList.size());
			System.out.println("Total Trajectories Collected (TP): " + trajectoryRDD.count());
			TOTAL_TRAJ_FILTERED += tIdList.size();
			TOTAL_PARTITIONS_FILTERED += filteredPartitionsRDD.count();
			System.out.println("TOTAIS: ");
			System.out.println("TOTAL TRAJ FILT.: " + TOTAL_TRAJ_FILTERED);
			System.out.println("TOTAL PARTITIONS FILT.: " + TOTAL_PARTITIONS_FILTERED);
			System.out.println("TOTAL PARTITIONS TO COL.: " + TOTAL_PARTITIONS_TO_COLLECT);*/
			
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
