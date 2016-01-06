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
			"hdfs://localhost:9000";

	// path to data locally
	static final String LOCAL_PATH =
			"file:/home/uqdalves/my-data";
	
	// path to the data set folders/files 
	static final String DATA_PATH = 
			LOCAL_PATH + "/trajectory-data/split1," + 
			LOCAL_PATH + "/trajectory-data/split2," +
			LOCAL_PATH + "/trajectory-data/split3," +
			LOCAL_PATH + "/trajectory-data/split4";
	
	// path to output folder inside HDFS
	static final String HDFS_OUTPUT = 
			"/spark-data/output/";
	
	// path to output log folder inside HDFS
	static final String APP_LOG = 
			"/spark-data/applog/";
	
	// Hadoop home path
	static final String HADOOP_HOME = 
			"/home/uqdalves/hadoop/hadoop-2.7.1";
	
	// the min number of partitions of the input data
	static final int NUM_PARTITIONS_DATA = 250; // number of data blocks
		
	// RDD storage level
	static final StorageLevel STORAGE_LEVEL= 
			StorageLevel.DISK_ONLY();
}
