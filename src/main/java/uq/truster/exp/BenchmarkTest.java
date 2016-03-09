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
import uq.spatial.Trajectory;
import uq.truster.partition.PartitionSeg;
import uq.truster.partition.PartitionSub;
import uq.truster.partition.SpatialPartitionModuleSeg;
import uq.truster.partition.SpatialPartitionModuleSub;
import uq.truster.partition.TrajectoryTrackTableSeg;
import uq.truster.partition.TrajectoryTrackTableSub;
import uq.truster.query.NearNeighbor;
import uq.truster.query.QueryProcessingModuleSeg;
import uq.truster.query.QueryProcessingModuleSub;

/**
 * Experiment to evaluate the correctness of the algorithm.
 * Generate log result to compare with the benchmark.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class BenchmarkTest implements Serializable, EnvironmentVariables, GeoInterface{
	// service to read files
	private static final FileReader reader = 
			new FileReader();
	// experiments log
	private static final Logger LOG = new Logger();
	
	/**
	 * Main: Benchmark comparison testing.
	 */
	public static void main(String[] arg0){
		System.out.println("\n[TRUSTER] Application Starts..\n");

		LOG.appendln("Truster Test Results.");
		LOG.appendln();
		
		/*****
		 * READ DATA AND CONVERT TO TRAJECTORIES 
		 *****/
		JavaRDD<Trajectory> trajectoryRDD = reader.readData();
		
		// create a grid for data partitioning
		Grid grid = new Grid(SIZE_X, SIZE_Y, MIN_X, MIN_Y, MAX_X, MAX_Y);
		
		/*****
		 * DATA PARTITIONING - SEGMENT -  TRUSTER SPATIAL PARTITION MODULE 
		 *****/
/*		// call partitioning module
		SpatialPartitionModuleSeg partitionModSeg = 
				new SpatialPartitionModuleSeg();
		JavaPairRDD<Integer, PartitionSeg> partitionsSegRDD = 
				partitionModSeg.partition(trajectoryRDD, grid);
		TrajectoryTrackTableSeg trackTableSeg = partitionModSeg.getTTT();		

		/*****
		 * DATA PARTITIONING - SUB-TRAJECTORY -  TRUSTER SPATIAL PARTITION MODULE 
		 *****/
		// call partitioning module
		SpatialPartitionModuleSub partitionModSub = 
				new SpatialPartitionModuleSub();
		JavaPairRDD<Integer, PartitionSub> partitionsSubRDD = 
				partitionModSub.partition(trajectoryRDD, grid);
		TrajectoryTrackTableSub trackTableSub = 
				partitionModSub.getTTT();	
		
		/*****
		 * QUERY PROCESSING - SEGMENT - TRUSTER QUERY PROCESSING MODULE 
		 *****/
/*		QueryProcessingModuleSeg queryModuleSeg = 
				new QueryProcessingModuleSeg(partitionsSegRDD, trackTableSeg, grid);
		
		/*****
		 * QUERY PROCESSING - SUB-TRAJECTORY - TRUSTER QUERY PROCESSING MODULE 
		 *****/
		QueryProcessingModuleSub queryModuleSub = 
				new QueryProcessingModuleSub(partitionsSubRDD, trackTableSub, grid);

		// Run spatial-temporal selection test
/*		List<STRectangle> stTestCases = 
				reader.readSpatialTemporalTestCases();
		{
			LOG.appendln("Spatial-Temporal Selection Result.");
			LOG.appendln();
			for(int i=1; i<=10; i++){ // run only 10 queries
				STRectangle stObj = stTestCases.get(i);
				// run query	
				List<Trajectory> tListResult = 
						queryModuleSeg.processSelectionQuery(stObj);	
				// count number of points returned (except bounday points)
				int count = 0;
				for(Trajectory t : tListResult){
					count += t.size();
				}
				LOG.appendln("Query " + i + " Result.");
				LOG.appendln("Number of Points: " + count); 
				LOG.appendln("Sub-Trajectories Returned: " + tListResult.size());
			}
		}
*/		
		// Run kNN test
		List<Trajectory> nnTestCases = 
				reader.readNearestNeighborTestCases();
		{
			LOG.appendln("K-NN Result.");
			LOG.appendln();
			for(int i=1; i<=10; i++){ // run only 10 queries
				// params
				Trajectory q = nnTestCases.get(i);
				long tIni = q.timeIni();
				long tEnd = q.timeEnd();
				final int k = 10; // 10-NN
				// run query
				List<NearNeighbor> resultList = 
						queryModuleSub.processKNNQuery(q, tIni, tEnd, k);
				LOG.appendln("Query " + i + " Result.");
				LOG.appendln("Query Trajectory: " + q.id);
				LOG.appendln("Trajectories Returned: " + resultList.size());
				int n=1;
				for(NearNeighbor nn : resultList){
					LOG.appendln(n++ + "-NN: " + nn.id);
					LOG.appendln("Dist: " + nn.distance);
				}
			}
		}

		// save the result log to HDFS
		LOG.save("truster-benchmark-selection-results");
		
		System.out.println("\n[TRUSTER] Application Ends..\n");
	}
}
