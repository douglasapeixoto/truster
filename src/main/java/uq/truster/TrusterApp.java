package uq.truster;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import uq.fs.FileToObjectRDDService;
import uq.spark.EnvironmentVariables;
import uq.spatial.Grid;
import uq.spatial.Trajectory;
import uq.truster.partition.Partition;
import uq.truster.partition.SpatialPartitionModule;

/**
 * Truster main app class
 */
public class TrusterApp implements EnvironmentVariables {
	// grid/space dimensions
	private final static double MIN_X = -100.0;
	private final static double MAX_X = +100.0;
	private final static double MIN_Y = -100.0;
	private final static double MAX_Y = +100.0;
	// number of grid partitions
	private final static int SIZE_X = 32;
	private final static int SIZE_Y = 32;
	
	public static void main(String[] arg){
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
		
		/*****
		 * PROCESS QUERIES - TRUSTER QUERY PROCESSING MODULE 
		 *****/
		// TODO: process queries using the partitionRDD
	}
	
	/**
	 * Read input dataset and convert to a RDD of trajectories
	 */
	public static JavaRDD<Trajectory> readData(){
		// read raw data to Spark RDD
		JavaRDD<String> fileRDD = SC.textFile(DATA_PATH);
		fileRDD.persist(STORAGE_LEVEL);
		
		// convert the input data to a RDD of trajectory objects
		FileToObjectRDDService rdd = new FileToObjectRDDService();
		JavaRDD<Trajectory> trajectoryRDD = 
				rdd.mapRawDataToTrajectoryRDD(fileRDD);
		trajectoryRDD.persist(STORAGE_LEVEL);
		
		return trajectoryRDD;
	}

}
