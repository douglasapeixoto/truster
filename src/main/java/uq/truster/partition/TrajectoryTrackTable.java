package uq.truster.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import uq.fs.HDFSFileService;
import uq.spark.EnvironmentVariables;
import uq.spatial.STSegment;

/**
 * Pair RDD to keep track of trajectories across partitions.
 * Pairs: <Trajectory Id, Grid index set>.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TrajectoryTrackTable implements Serializable, EnvironmentVariables {
	/**
	 * The RDD of this table object hash table: 
	 * (Trajectory ID, Set of grid index)
	 */
	private JavaPairRDD<String, HashSet<Integer>> trajectoryTrackTableRDD = null;
	
	/**
	 * Build the Trajectory Track table (TTT). 
	 * </br>
	 * Assign each trajectory to a set of grid indexes
	 * it overlaps with. 
	 * </br>
	 * Build a RDD with key-value pairs:
	 * (TrajectoryID, Set of grid index)
	 */
	public void build(
			final JavaPairRDD<Integer, STSegment> segmentToGridIndexRDD){	
		// Map segments to overlapping grids.
		// Map each pair (Grid index, Segment) to (TrajectoryID, Grid index set)
		mapSegmentsToGridIndexSet(segmentToGridIndexRDD);
		trajectoryTrackTableRDD.setName("TrajectoryTrackTable");
		trajectoryTrackTableRDD.persist(STORAGE_LEVEL);
	}

	/**
	 * Persist this table object, set in the specified Storage Level:
	 * MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, etc.
	 */
	public void persist(StorageLevel level){
		trajectoryTrackTableRDD.persist(level);
	}
	
	/**
	 * Remove this RDD from the storage level.
	 * Clean the cache.
	 */
	public void unpersist(){
		trajectoryTrackTableRDD.unpersist();
	}
	
	/**
	 * The number of trajectories (rows) in this
	 * table RDD.
	 */
	public long count(){
		return trajectoryTrackTableRDD.count();
	}
	
	/**
	 * Return all grid indexes for a given trajectory.
	 * Filter all grid indexes that contain the given trajectory.
	 * 
	 * @return Return a set of partition page Indexes <VSI = PivotID, TPI = TimePage>.
	 */
	public HashSet<Integer> collectIndexListByTrajectoryId(
			final String trajectoryId){
		// Filter tuple with key = trajectoryId
		JavaRDD<HashSet<Integer>> filteredRDD = trajectoryTrackTableRDD.filter(
				new Function<Tuple2<String,HashSet<Integer>>, Boolean>() {
			public Boolean call(Tuple2<String, HashSet<Integer>> tuple) throws Exception {
				return trajectoryId.equals(tuple._1);
			}
		}).values();
		HashSet<Integer> indexSet = new HashSet<Integer>();
		if(!filteredRDD.isEmpty()){
			indexSet = filteredRDD.collect().get(0);
		}
		return indexSet; 
	}

	/**
	 * Return all grid indexes for a given trajectory set.
	 * </br>
	 * Collect all grid indexes that contain any of the 
	 * trajectories in the set.
	 * 
	 * @return Return a set of grid indexes.
	 */
	public HashSet<Integer> collectIndexListByTrajectoryId(
			final Collection<String> trajectoryIdSet){
		// Filter tuples
		JavaRDD<HashSet<Integer>> filteredRDD = 
			trajectoryTrackTableRDD.filter(new Function<Tuple2<String,HashSet<Integer>>, Boolean>() {
				public Boolean call(Tuple2<String, HashSet<Integer>> tuple) throws Exception {
					return trajectoryIdSet.contains(tuple._1);
				}
				// collect and merge tuple values
			}).values();
		
		HashSet<Integer> indexSet = new HashSet<Integer>();
		if(filteredRDD.isEmpty()){
			// return empty
			return indexSet;
		} else {
			indexSet = 
				filteredRDD.reduce(new Function2<HashSet<Integer>, HashSet<Integer>, HashSet<Integer>>() {
					public HashSet<Integer> call(
							HashSet<Integer> indexSet1, 
							HashSet<Integer> indexSet2) throws Exception {
						indexSet1.addAll(indexSet2);
						return indexSet1;
					}
				});
			return indexSet; 
		}
	}

	/**
	 * Count the number of grids by trajectory ID.
	 * 
	 * @return Return a pair RDD from trajectory IDs 
	 * to number of pages.
	 */
	public JavaPairRDD<String, Integer> countByTrajectoryId(){
		// map each tuple (trajectory) to its number of pages
		JavaPairRDD<String, Integer> countByKeyRDD = 
			trajectoryTrackTableRDD.mapToPair(
					new PairFunction<Tuple2<String,HashSet<Integer>>, String, Integer>() {
				public Tuple2<String, Integer> call(Tuple2<String, HashSet<Integer>> tuple) throws Exception {
					return new Tuple2<String, Integer>(tuple._1, tuple._2.size());
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer v1, Integer v2) throws Exception {
					return (v1 + v2);
				}
			});
		
		return countByKeyRDD;
	}
	
	/**
	 * Return a vector with some statistical information about
	 * the number of grids per trajectory in this RDD.
	 * 
	 * @return A double vector containing the mean: [0], min: [1],
	 * max: [2], and std: [3] number of pages per trajectory
	 * in this RDD.
	 */
	public double[] pagesPerTrajectoryInfo(){
		// total number of tuple
		final double total = count();
		
		// get mean, min, max and std number of pages per trajectory
		double[] count = 
			trajectoryTrackTableRDD.values().glom().map(
					new Function<List<HashSet<Integer>>, double[]>() {
				public double[] call(List<HashSet<Integer>> list) throws Exception {
					double[] vec = new double[]{0.0,INF,0.0,0.0};
					for(HashSet<Integer> set : list){
						long count = set.size();
						vec[0] += count;
						vec[1] = Math.min(vec[1], count); 
						vec[2] = Math.max(vec[2], count); 
						vec[3] += (count*count);
					}
					return vec;
				}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] vec1, double[] vec2) throws Exception {
					vec1[0] = vec1[0] + vec2[0];
					vec1[1] = Math.min(vec1[1], vec2[1]);
					vec1[2] = Math.max(vec1[2], vec2[2]);
					vec1[3] = vec1[3] + vec2[3];
					return vec1;
				}
			});
		// get std
		count[3] = (count[3] - 
				(1/total)*(count[0]*count[0]));
		count[3] = Math.sqrt(count[3]/total);
		// get mean
		count[0] = count[0]/total;
		
		return count;		
	}
	
	/**
	 * The number of partitions of this table RDD.
	 */
	public long getNumPartitions(){
		return trajectoryTrackTableRDD.partitions().size();
	}

	/**
	 * Save track table statistical information. 
	 * Save to HDFS output folder as "trajectory-track-table-info"
	 */
	public void saveTableInfo(){
		double[] tupleInfo = pagesPerTrajectoryInfo();
		
		List<String> info = new ArrayList<String>();
		info.add("Number of Table Tuples: " + count());
		info.add("Number of RDD Partitions: " + getNumPartitions());
		info.add("Avg. Pages per Trajectory: " + tupleInfo[0]);
		info.add("Min. Pages per Trajectory: " + tupleInfo[1]);
		info.add("Max. Pages per Trajectory: " + tupleInfo[2]);
		info.add("Std. Pages per Trajectory: " + tupleInfo[3]);

		// save to hdfs
		HDFSFileService hdfs = new HDFSFileService();
		hdfs.saveStringListHDFS(info, "trajectory-track-table-info");
	}

	/**
	 * A MapRedcuce/Aggregate function to assign each trajectory to its 
	 * overlapping pages.
	 * </br>
	 * Return a RDD of pairs: (TrajectoryID, Set of PagesIndex)
	 */
	private JavaPairRDD<String, HashSet<Integer>> mapSegmentsToGridIndexSet(
			final JavaPairRDD<Integer, STSegment> segmentToGridIndex){
		
		// map each segment to a pair (TrajectoryID, Grid index)
		JavaPairRDD<String, Integer> idToGridIndexSetRDD = 
				segmentToGridIndex.mapToPair(
				new PairFunction<Tuple2<Integer,STSegment>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, STSegment> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._2.parentId, tuple._1);
			}
		});
		
		// an empty index set to start aggregating
		HashSet<Integer> emptySet = new HashSet<Integer>();
		// aggregate functions
		Function2<HashSet<Integer>, Integer, HashSet<Integer>> seqFunc = 
				new Function2<HashSet<Integer>, Integer, HashSet<Integer>>() {
			@Override
			public HashSet<Integer> call(HashSet<Integer> set, Integer index) throws Exception {
				set.add(index);
				return set;
			}
		};
		Function2<HashSet<Integer>, HashSet<Integer>, HashSet<Integer>> combFunc = 
				new Function2<HashSet<Integer>, HashSet<Integer>, HashSet<Integer>>() {
			@Override
			public HashSet<Integer> call(HashSet<Integer> set1, HashSet<Integer> set2) throws Exception {
				set1.addAll(set2);
				return set1;
			}
		};

		// aggregates the index sets by trajectory ID
		trajectoryTrackTableRDD = 
				idToGridIndexSetRDD.aggregateByKey(emptySet, seqFunc, combFunc);
			
		return trajectoryTrackTableRDD;
	}
}
