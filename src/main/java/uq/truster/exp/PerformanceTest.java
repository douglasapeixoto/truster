package uq.truster.exp;
 
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import uq.fs.FileReader;
import uq.spark.EnvironmentVariables;
import uq.spark.Logger;
import uq.spatial.GeoInterface;
import uq.spatial.Grid;
import uq.spatial.STRectangle;
import uq.spatial.Trajectory;
import uq.truster.partition.PartitionSub;
import uq.truster.partition.SpatialPartitionModuleSub;
import uq.truster.partition.TrajectoryTrackTableSub;
import uq.truster.query.NearNeighbor;
import uq.truster.query.QueryProcessingModuleSub;

/**
 * Experiment to evaluate the performance of the algorithm.
 * Generate log result with query performance information.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PerformanceTest  implements Serializable, EnvironmentVariables, GeoInterface {
	// service to read files
	private static final FileReader reader = 
			new FileReader();
	// experiments log
	private static final Logger LOG = new Logger();
	// result file name
	private static final String EXP_NAME = 
			"truster-mem-performance";
	
	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\n[TRUSTER] Application Starts..\n");

		/*****
		 * READ DATA AND CONVERT TO TRAJECTORIES 
		 *****/
		JavaRDD<Trajectory> trajectoryRDD = reader.readData();
		
		// create a grid for data partitioning
		Grid grid = new Grid(SIZE_X, SIZE_Y, MIN_X, MIN_Y, MAX_X, MAX_Y);

		/*****
		 * DATA PARTITIONING - TRUSTER SPATIAL PARTITION MODULE 
		 *****/
		// call partitioning module
		SpatialPartitionModuleSub partitionModSub = 
				new SpatialPartitionModuleSub();
		JavaPairRDD<Integer, PartitionSub> partitionsSubRDD = 
				partitionModSub.partition(trajectoryRDD, grid);
		TrajectoryTrackTableSub trackTableSub = 
				partitionModSub.getTTT();	
		
		// action to force building the index
		System.out.println("Num Partitions: " + partitionsSubRDD.count());
		System.out.println("Num TTT tuples: " + trackTableSub.count());
		
		/*****
		 * QUERY PROCESSING - TRUSTER QUERY PROCESSING MODULE 
		 *****/
		QueryProcessingModuleSub queryModuleSub = 
				new QueryProcessingModuleSub(partitionsSubRDD, trackTableSub, grid);
		
		// SPATIAL-TEMPORAL SELECTION QUERIES (EXACT)
		List<STRectangle> stUseCases = reader.readSpatialTemporalTestCases();
		{
			LOG.appendln("Spatial-Temporal Selection Query Result:\n");
			long selecQueryTime=0;
			int queryId=1;
			for(STRectangle stObj : stUseCases){
				System.out.println("\nQuery " + queryId);
				long start = System.currentTimeMillis();
				// run query - exact sub-trajectories
				List<Trajectory> tListResult = 
						queryModuleSub.processSelectionQuery(stObj);	
				long time = System.currentTimeMillis()-start;
				LOG.appendln("Query " + queryId++ + ": " + tListResult.size() + " sub-trajectories in " + time + " ms.");
				selecQueryTime += time;		
			}
			LOG.appendln("Spatial-Temporal Selection (Exact) ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total Spatial-Temporal Selection Query Time: " + selecQueryTime + " ms.\n");
		}
		
		// NN QUERIES
/*		List<Trajectory> nnTestCases = 
				reader.readNearestNeighborTestCases();
		{
			LOG.appendln("NN Query Result:\n");
			long nnQueryTime=0;
			int queryId=1;
			for(Trajectory t : nnTestCases){
				System.out.println("\nQuery " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				Trajectory nnResult =
						queryModuleSub.processNNQuery(t, tIni, tEnd);
				long time = System.currentTimeMillis() - start;
				LOG.appendln("NN Query " + queryId++ + ": " +  nnResult.id + " in " + time + " ms.");
				nnQueryTime += time;
			}
			LOG.appendln("NN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total NN Time: " + nnQueryTime + " ms.\n");
		}

		// K-NN QUERIES
/*		{
			LOG.appendln("K-NN Query Result:\n");
			long nnQueryTime=0;
			int queryId=1;
			final int k = 10;
			for(Trajectory t : nnTestCases){
				System.out.println("\nQuery " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				List<NearNeighbor> resultList = 
						queryModuleSub.processKNNQuery(t, tIni, tEnd, k);
				long time = System.currentTimeMillis() - start;
				LOG.appendln(k + "-NN Query " + queryId++ + ": " +  resultList.size() + " in " + time + " ms.");
				nnQueryTime += time;
			}
			LOG.appendln(k + "-NN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total " + k + "-NN Time: " + nnQueryTime + " ms.\n");
		}
	
		// save the result log to HDFS
		LOG.save(EXP_NAME);
*/
		System.out.println("\n[TRUSTER] Application Ends..\n");
	}
}
