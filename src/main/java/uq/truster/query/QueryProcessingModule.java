package uq.truster.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import uq.spatial.Grid;
import uq.spatial.STRectangle;
import uq.spatial.STSegment;
import uq.spatial.Trajectory;
import uq.truster.partition.Partition;

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
	
	/**
	 * Set the data partition RDD and the grid used to process queries.
	 */
	public QueryProcessingModule(
			JavaPairRDD<Integer, Partition> partitionsRDD, Grid grid) {
		this.partitionsRDD = partitionsRDD;
		this.grid = grid;
	}

	/**
	 * Given a spatial-temporal query region (spatial-temporal rectangle),
	 * returns from the partition RDD all segments that satisfy the query, 
	 * that is, all segments covered by the query area within the query 
	 * time interval.
	 * 
	 * @return A list of segments that satisfy the query.
	 **/
	public List<Trajectory> processQuery(
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
}
