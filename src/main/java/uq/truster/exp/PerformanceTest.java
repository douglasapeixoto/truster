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
import uq.truster.partition.PartitionSeg;
import uq.truster.partition.SpatialPartitionModuleSeg;
import uq.truster.partition.TrajectoryTrackTableSeg;
import uq.truster.query.NearNeighbor;
import uq.truster.query.QueryProcessingModuleSeg;

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
	
	/**
	 * Main: Performance testing.
	 */
	public static void main(String[] args){
		System.out.println("\n[TRUSTER] Application Starts..\n");

		/*****
		 * READ DATA AND CONVERT TO TRAJECTORIES 
		 *****/
		JavaRDD<Trajectory> trajectoryRDD = reader.readData();

		/*****
		 * DATA PARTITIONING - TRUSTER SPATIAL PARTITION MODULE 
		 *****/
		// create a grid for data partitioning
		Grid grid = new Grid(SIZE_X, SIZE_Y, MIN_X, MIN_Y, MAX_X, MAX_Y);
		// call partitioning module
		SpatialPartitionModuleSeg partitionMod = new SpatialPartitionModuleSeg();
		JavaPairRDD<Integer, PartitionSeg> partitionsRDD = 
				partitionMod.partition(trajectoryRDD, grid);
		TrajectoryTrackTableSeg trackTable = partitionMod.getTTT();
		
		// action to force  to building the partitions
		System.out.println("[TRUSTER] Num. Partitions: " + partitionsRDD.count());
		
		/*****
		 * QUERY PROCESSING - TRUSTER QUERY PROCESSING MODULE 
		 *****/
		QueryProcessingModuleSeg queryModule = 
				new QueryProcessingModuleSeg(partitionsRDD, trackTable, grid);
		
		// SPATIAL-TEMPORAL SELECTION QUERIES (EXACT)
		List<STRectangle> stUseCases = reader.readSpatialTemporalTestCases();
		{
			LOG.appendln("Spatial-Temporal Selection Query Result:\n");
			long selecQueryTime=0;
			int queryId=1;
			for(STRectangle stObj : stUseCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();
				// run query - exact sub-trajectories
				List<Trajectory> tListResult = 
						queryModule.processSelectionQuery(stObj);	
				long time = System.currentTimeMillis()-start;
				LOG.appendln("Query " + queryId++ + ": " + tListResult.size() + " sub-trajectories in " + time + " ms.");
				selecQueryTime += time;		
			}
			LOG.appendln("Spatial-Temporal Selection (Exact) ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total Spatial-Temporal Selection Query Time: " + selecQueryTime + " ms.\n");
		}		
		// NN QUERIES
		List<Trajectory> nnTestCases = 
				reader.readNearestNeighborTestCases();
		{
			LOG.appendln("NN Query Result:\n");
			long nnQueryTime=0;
			int queryId=1;
			for(Trajectory t : nnTestCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				Trajectory nnResult =
						queryModule.processKNNQuery(t, tIni, tEnd, 1).get(0);
				long time = System.currentTimeMillis() - start;
				LOG.appendln("NN Query " + queryId++ + ": " +  nnResult.id + " in " + time + " ms.");
				nnQueryTime += time;
			}
			LOG.appendln("NN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total NN Time: " + nnQueryTime + " ms.\n");
		}
		// K-NN QUERIES
		{
			LOG.appendln("K-NN Query Result:\n");
			long nnQueryTime=0;
			int queryId=1;
			final int k = 10;
			for(Trajectory t : nnTestCases){
				System.out.println("Query " + queryId);
				long start = System.currentTimeMillis();				
				// run query
				long tIni = t.timeIni();
				long tEnd = t.timeEnd();
				List<NearNeighbor> resultList = 
						queryModule.processKNNQuery(t, tIni, tEnd, k);
				long time = System.currentTimeMillis() - start;
				LOG.appendln(k + "-NN Query " + queryId++ + ": " +  resultList.size() + " in " + time + " ms.");
				// print result
				/*int i=1;
				for(NearNeighbor nn : resultList){
					LOG.appendln(i++ + "-NN: " + nn.id);
				}*/
				nnQueryTime += time;
			}
			LOG.appendln(k + "-NN query ends at: " + System.currentTimeMillis() + "ms.");
			LOG.appendln("Total " + k + "-NN Time: " + nnQueryTime + " ms.\n");
		}
		
		// save the result log to HDFS
		LOG.save("truster-performance-results");
		
		System.out.println("\n[TRUSTER] Application Ends..\n");
	}
		
}
