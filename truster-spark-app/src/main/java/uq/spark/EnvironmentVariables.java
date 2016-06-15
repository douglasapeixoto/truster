package uq.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Setup environment configuration variables.
 * 
 * @author uqdalves
 *
 */
public interface EnvironmentVariables {
	// Spark context
	static final JavaSparkContext SC = 
			MySparkContext.getInstance();
	
	// path to HDFS
	static final String HDFS_PATH =
			"hdfs://master:54310";      // cluster
			//"hdfs://localhost:9000";  // local
	
	// path to data locally
	static final String LOCAL_PATH =
			"file:/home/uqdalves/my-data";
	
	// path to the data set folders/files 
	static final String DATA_PATH = 
			HDFS_PATH + "/spark-data/trajectory-data/split1," + 
			HDFS_PATH + "/spark-data/trajectory-data/split2," +
			HDFS_PATH + "/spark-data/trajectory-data/split3," +
			HDFS_PATH + "/spark-data/trajectory-data/split4";
			/*
			LOCAL_PATH + "/trajectory-data/split1," + 
			LOCAL_PATH + "/trajectory-data/split2," +
			LOCAL_PATH + "/trajectory-data/split3," +
			LOCAL_PATH + "/trajectory-data/split4";*/
			
	// path to output folder inside HDFS
	static final String HDFS_OUTPUT = 
			"/spark-data/output/";
	
	// path to output log folder inside HDFS
	static final String APP_LOG = 
			"/spark-data/applog/";
	
	// Hadoop home path
	static final String HADOOP_HOME = 
			"/usr/share/hadoop/hadoop-2.7.1";    // Cluster
			//"/home/uqdalves/hadoop/hadoop-2.7.1";  // Local
	
	// the min number of partitions of the input data
	static final int NUM_PARTITIONS_DATA = 250; // number of data blocks
	
	// number of reduce tasks for the indexing process
	static final int NUM_PARTITIONS_RDD = NUM_PARTITIONS_DATA * 4;
		
	// number of reduce tasks for the indexing process
	static final int NUM_PARTITIONS_TTT = NUM_PARTITIONS_DATA / 2;	
	
	// number of partitions to coalesce after filtering
	static final int COALESCE_NUMBER = NUM_PARTITIONS_DATA / 5;
	
	// RDD storage level
	static final StorageLevel STORAGE_LEVEL = 
			StorageLevel.MEMORY_ONLY();
	
	// RDD storage level of the Trajectory Track Table
	static final StorageLevel STORAGE_LEVEL_TTT = 
			StorageLevel.MEMORY_ONLY();

	// an infinity value
	static final double INF = Double.MAX_VALUE;

	// number of grid partitions
	final static int SIZE_X = 550; //32;
	final static int SIZE_Y = 550; //32;
}
