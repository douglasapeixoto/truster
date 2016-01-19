package uq.exp;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import uq.fs.FileReaderService; 
import uq.fs.HDFSFileService;
import uq.spark.Logger;
import uq.spark.EnvironmentVariables;
import uq.spatial.GeoInterface;
import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.STRectangle;
import uq.spatial.Trajectory;
import uq.truster.partition.Partition;
import uq.truster.partition.SpatialPartitionModule;
import uq.truster.partition.TrajectoryTrackTable;
import uq.truster.query.NearNeighbor;
import uq.truster.query.QueryProcessingModule;

/**
 * Service to perform experiments
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class ExperimentsService implements Serializable, EnvironmentVariables, GeoInterface {
	private static final HDFSFileService HDFS = new HDFSFileService();
	// experiments log
	private static final Logger LOG = new Logger();
	// experiment log file name
	private static final String LOG_NAME = 
			"experiments-truster-";

	/**
	 * Main
	 */
	public static void main(String[] args){
		System.out.println("\nRunning Experiments..\n");
		
		/*****
		 * READ DATA AND CONVERT TO TRAJECTORIES 
		 *****/
		JavaRDD<Trajectory> trajectoryRDD = readData();
		
		/*****
		 * CREATE A GRID FOR PARTITIONING THE DATA 
		 *****/
		Grid grid = new Grid(SIZE_X, SIZE_Y, MIN_X, MIN_Y, MAX_X, MAX_Y);
		
		/*****
		 * PARTITION THE DATA - TRUSTER SPATIAL PARTITION MODULE 
		 *****/
		SpatialPartitionModule partitionMod = new SpatialPartitionModule();
		JavaPairRDD<Integer, Partition> partitionsRDD = 
				partitionMod.partition(trajectoryRDD, grid);
		TrajectoryTrackTable trackTable = partitionMod.getTTT();
		
		// action to force  to building the partitions
		System.out.println("Num. Partitions: " + partitionsRDD.count());
		
		/*****
		 * PROCESS QUERIES - TRUSTER QUERY PROCESSING MODULE 
		 *****/
		QueryProcessingModule queryModule = 
				new QueryProcessingModule(partitionsRDD, trackTable, grid);
		
		/******
		 * SPATIAL-TEMPORAL SELECTION QUERIES (EXACT)
		 ******/
		List<STRectangle> stUseCases = readSpatialTemporalTestCases();
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
		/******
		 * NN QUERIES
		 ******/
		List<Trajectory> nnTestCases = readNearestNeighborTestCases();
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
		/******
		 * K-NN QUERIES
		 ******/
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
		LOG.save(LOG_NAME);
	}
		
	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	public static List<STRectangle> readSpatialTemporalTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/use-cases/spatial-temporal-use-cases");
		// process lines
		long timeIni, timeEnd;
		double left, right, bottom, top;
		List<STRectangle> stList = new LinkedList<STRectangle>(); 
		for(String line : lines){
			if(line.length() > 3){
				String[] tokens = line.split(" ");
				left	= Double.parseDouble(tokens[0]);
				right	= Double.parseDouble(tokens[1]);
				bottom	= Double.parseDouble(tokens[2]);
				top		= Double.parseDouble(tokens[3]);
				timeIni = Long.parseLong(tokens[4]);
				timeEnd = Long.parseLong(tokens[5]); 
				
				stList.add(new STRectangle(left, bottom, right, top, timeIni, timeEnd));
			}
		}
		return stList;
	}
	
	/**
	 * Read the uses cases for Nearest Neighbors queries.
	 */
	public static List<Trajectory> readNearestNeighborTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/use-cases/nn-use-cases");
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

	/**
	 * Read input dataset and convert to a RDD of trajectories
	 */
	private static JavaRDD<Trajectory> readData(){
		System.out.println("Reading data..");
		
		// read raw data to Spark RDD
		JavaRDD<String> fileRDD = SC.textFile(DATA_PATH);
		fileRDD.persist(STORAGE_LEVEL);
		
		// convert the input data to a RDD of trajectory objects
		FileReaderService rdd = new FileReaderService();
		JavaRDD<Trajectory> trajectoryRDD = 
				rdd.mapRawDataToTrajectoryRDD(fileRDD);
		trajectoryRDD.persist(STORAGE_LEVEL);
		
		return trajectoryRDD;
	}
}
