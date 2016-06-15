package uq.truster.exp;

import java.io.Serializable; 
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.broadcast.Broadcast;
 
import uq.fs.HDFSFileService;
import uq.spark.EnvironmentVariables;
import uq.spark.Logger;
import uq.spatial.GeoInterface;
import uq.spatial.Grid; 
import uq.spatial.Point;
import uq.spatial.STRectangle;
import uq.spatial.Trajectory;
import uq.truster.partition.PartitionSub;
import uq.truster.partition.SpatialPartitioningModuleSub;
import uq.truster.partition.TrajectoryTrackTableSub; 
import uq.truster.query.QueryProcessingModuleSub;

/**
 * Experiment to evaluate throughput of the algorithm (query/minute).
 * Generate log result with query performance information.
 * </br>
 * Process multiple concurrent Threads (multithreading - 
 * multiple queries  using Spark's FAIR scheduling).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PerformanceConcurrentTest implements Serializable, EnvironmentVariables, GeoInterface {
	// access to HDFS
	private static final HDFSFileService HDFS = 
			new HDFSFileService();
 	// maximum number of concurrent Threads
	private static final int NUM_THREADS = 5;
	// the time threshold to terminate the concurrent queries (in milliseconds)
	private static final int TERMINATE_TIME = 300; // minutes
	// experiments log
	private static final Logger LOG = new Logger();
	// query finish count
	private static int QUERY_COUNT = 0;
	// experiment log file name
	private static final String LOG_NAME = 
			"truster-concurrent-" + SIZE_X + "x" + SIZE_Y;
	
	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\n[TRUSTER] Running Concurrent Performance Test..");
		System.out.println("\n[TRUSTER] Module Starts At "
				+ System.currentTimeMillis() + " ms.\n");

		/*****
		 * DATA PARTITIONING - TRUSTER SPATIAL PARTITION MODULE 
		 *****/
		// call partitioning module
		SpatialPartitioningModuleSub partitionModSub = 
				new SpatialPartitioningModuleSub();

		// run data partitioning and indexing
		partitionModSub.run();
		
		// create a grid for data partitioning 
		final Broadcast<Grid> grid = partitionModSub.getGridDiagram();
		final JavaPairRDD<Integer, PartitionSub> partitionsSubRDD = 
				partitionModSub.getPartitionsRDD();
		final TrajectoryTrackTableSub trackTableSub = 
				partitionModSub.getTrajectoryTrackTable();
		
		// action to force building the index
		System.out.println("Num. Partitions: " + partitionsSubRDD.count());
		System.out.println("Num. TTT tuples: " + trackTableSub.count());		

		/************************
		 * QUERIES INITIALIZATION 
		 ************************/
		QueryProcessingModuleSub queryModuleSub = 
				new QueryProcessingModuleSub(partitionsSubRDD, trackTableSub, grid);
		
		// ST-SELECTION QUERY
	    // run the first query - non-concurrent - force to build the RDD
/*	    final List<STRectangle> stTestCases = readSpatialTemporalTestCases(); // test cases
		List<Trajectory> first_result = queryModuleSub.processSelectionQuery(stTestCases.get(0));
		System.out.println("First Query Result Size: " + first_result.size());
*/		
		// K-NN QUERY
	    final List<Trajectory> nnUseCases = readNearestNeighborTestCases(); // test cases
	    Trajectory first_nn_result = queryModuleSub.processNNQuery(
	    		nnUseCases.get(0), nnUseCases.get(0).timeIni(), nnUseCases.get(0).timeEnd());
		System.out.println("First Query Result Size: " + first_nn_result.id);
		
		/************************
		 * MULTI-THREAD QUERIES SETUP 
		 ************************/
		// concurrent threads start
	    long startTime = System.currentTimeMillis();
	    System.out.println("[TRUSTER] Concurrent Threads starts at: " + startTime + " ms.");
	    // At any point, at most NUM_THREADS will be active processing tasks.
	    ExecutorService executorService = 
	    		Executors.newFixedThreadPool(NUM_THREADS);
	    
		/******
		 * SPATIAL TEMPORAL SELECTION QUERIES (EXACT)
		 ******/ 
/*	    LOG.appendln("Spatial-Temporal Selection Concurrent Query Result (Exact):\n");
	    LOG.appendln("Queries Start at: " + startTime + " ms.\n");
	    // create one thread per query
	    for(int i=1; i<stTestCases.size(); i++){
	    	final STRectangle stQueryObj = stTestCases.get(i);
	    	// thread starts
	    	executorService.submit(new Runnable() {
				public void run() {
					long qStart = System.currentTimeMillis();
					// run query - exact selection trajectories
					QueryProcessingModuleSub queryModule = 
							new QueryProcessingModuleSub(partitionsSubRDD, trackTableSub, grid);
					List<Trajectory> result = 
							queryModule.processSelectionQuery(stQueryObj);
					// query finish
					int resultSize = result.size();
					long qEnd = System.currentTimeMillis();
					// add result to log
					addSelectionResultLog(qStart, qEnd, resultSize);
					System.out.println("[THREAD] Query Time: " + (qEnd-qStart) + " ms.");
				}
			});
	    }
*/
		/******
		 * NN QUERIES
		 ******/
	    LOG.appendln("Nearest-Neighbor Concurrent Query Result:\n");
	    LOG.appendln("Queries Start at: " + startTime + " ms.\n");
	    // create one thread per query
	    for(int i=1; i<nnUseCases.size(); i++){
	    	final Trajectory nnQueryObj = nnUseCases.get(i);
	    	// thread starts
	    	executorService.submit(new Runnable() {
				public void run() {
					long qStart = System.currentTimeMillis();
					// run query - NN trajectories
					QueryProcessingModuleSub queryModule = 
							new QueryProcessingModuleSub(partitionsSubRDD, trackTableSub, grid);
					long tIni = nnQueryObj.timeIni();
					long tEnd = nnQueryObj.timeEnd();
					Trajectory result = queryModule.processNNQuery(nnQueryObj, tIni, tEnd);
					// query finish
					String resultId = result.id;
					long qEnd = System.currentTimeMillis();
					// add result to log
					addNNResultLog(nnQueryObj.id, qStart, qEnd, resultId);
					System.out.println("[THREAD] Query Time: " + (qEnd-qStart) + " ms.");
				}
			});
	    }
	    
		/******
		 * K-NN QUERIES
		 ******/
/*	    LOG.appendln("k-Nearest-Neighbors Concurrent Query Result:\n");
	    LOG.appendln("Queries Start at: " + startTime + " ms.\n");
	    // create one thread per query
	    final int k = 10; // number of neighbors
	    for(int i=1; i<nnUseCases.size(); i++){
	    	final Trajectory nnQueryObj = nnUseCases.get(i);
	    	// thread starts
	    	executorService.submit(new Runnable() {
				public void run() {
					long qStart = System.currentTimeMillis();
					// run query - NN trajectories
					QueryProcessingModuleSub queryModule = 
							new QueryProcessingModuleSub(partitionsSubRDD, trackTableSub, grid);
					long tIni = nnQueryObj.timeIni();
					long tEnd = nnQueryObj.timeEnd();
					List<NearNeighbor> result = 
							queryModule.processKNNQuery(nnQueryObj, tIni, tEnd, k);
					// query finish
					int resultSize = result.size();
					long qEnd = System.currentTimeMillis();
					// add result to log
					addNNResultLog(nnQueryObj.id, qStart, qEnd, resultSize+" trajectories ");
					System.out.println("[THREAD] Query Time: " + (qEnd-qStart) + " ms.");
				}
			});
	   }
*/
	   // await the Threads to finish execution, or wait TERMINATE_TIME, whichever happens first.
	   try {
		   	executorService.shutdown();
			executorService.awaitTermination(TERMINATE_TIME, TimeUnit.MINUTES);
			System.out.println("\n[TRUSTER] Module Ends At "
						+ System.currentTimeMillis() + " ms.\n");	
			// save the result log to HDFS
			LOG.appendln();
			LOG.appendln("Max. Number of Concurrent Queries: " + NUM_THREADS);
			LOG.append  ("Total Queries Finished: " + QUERY_COUNT);
			LOG.save(LOG_NAME);
			// unpersist RDDs
			partitionsSubRDD.unpersist();
			trackTableSub.unpersist();
		} catch (InterruptedException e) {
			System.out.println("[TRUSTER] Threw an "
					+ "'InterruptedException'!");
			e.printStackTrace();
		}
	}
		
	
	/**
	 * Append the result of each Selection query thread to the log
	 */
	public static /*synchronized*/ void addSelectionResultLog(long start, long end, int resultSize){
		QUERY_COUNT++;
		LOG.appendln("Query " + QUERY_COUNT + ": " + resultSize + " trajectories in " + (end-start) + " ms.");
		LOG.appendln("Query ends at: " + end + " ms.");
	}
	
	/**
	 * Append the result of each NN query thread to the log
	 */
	public static /*synchronized*/ void addNNResultLog(String queryId, long start, long end, String resultId){
		QUERY_COUNT++;
		LOG.appendln("Query " + queryId + ": returned " + resultId + " in " + (end-start) + " ms.");
		LOG.appendln("Query ends at: " + end + " ms.");
	}
	
	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	public static List<STRectangle> readSpatialTemporalTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/test-cases/spatial-temporal-test-cases");
		// process lines
		long t1, t2, min_t, max_t;
		double x1, x2, y1, y2;
		double min_x, min_y, max_x, max_y;
		List<STRectangle> stList = new LinkedList<STRectangle>(); 
		for(String line : lines){
			if(line.length() > 3){
				// read parameters
				String[] tokens = line.split(" ");
				x1 = Double.parseDouble(tokens[0]);
				x2 = Double.parseDouble(tokens[1]);
				y1 = Double.parseDouble(tokens[2]);
				y2 = Double.parseDouble(tokens[3]);
				t1 = Long.parseLong(tokens[4]);
				t2 = Long.parseLong(tokens[5]); 
				// get min/max (fix)
				min_x = Math.min(x1, x2);	max_x = Math.max(x1, x2);
				min_y = Math.min(y1, y2);	max_y = Math.max(y1, y2);
				min_t = Math.min(t1, t2);	max_t = Math.max(t1, t2);
				stList.add(new STRectangle(min_x, min_y, max_x, max_y, min_t, max_t));
			}
		}
		return stList;
	}
	
	/**
	 * Read the uses cases for Nearest Neighbors queries.
	 */
	public static List<Trajectory> readNearestNeighborTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/test-cases/nn-test-cases");
		// process lines
		int id=1;
		double x, y;
		long time;
		List<Trajectory> list = new LinkedList<Trajectory>();
		for(String line : lines){
			if(line.length() > 4){
				String[] tokens = line.split(" ");
				// first tokens is the id
				Trajectory t = new Trajectory("Q" + id++);
				for(int i=1; i<=tokens.length-3; i+=3){
					x = Double.parseDouble(tokens[i]);
					y = Double.parseDouble(tokens[i+1]);
					time = Long.parseLong(tokens[i+2]);
					t.addPoint(new Point(x, y, time));
				}
				list.add(t);
			}
		}
		return list;
	}
}
