package uq.fs;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import uq.spark.EnvironmentVariables;
import uq.spatial.Point;
import uq.spatial.STRectangle;
import uq.spatial.Trajectory;

/**
 * Service to read data files (Spark).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class FileReader implements Serializable, EnvironmentVariables{
	private static final HDFSFileService HDFS = 
			new HDFSFileService();

	/**
	 * Read the uses cases for spatial-temporal selection queries
	 */
	public List<STRectangle> readSpatialTemporalTestCases(){
		List<String> lines = 
				HDFS.readFileHDFS("/spark-data/test-cases/spatial-temporal-test-cases");
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
	public List<Trajectory> readNearestNeighborTestCases(){
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

	/**
	 * Read input dataset and convert to a RDD of trajectories
	 */
	public JavaRDD<Trajectory> readData(){
		System.out.println("[TRUSTER] Reading data..");
		
		// read raw data to Spark RDD
		JavaRDD<String> fileRDD = SC.textFile(DATA_PATH, NUM_PARTITIONS_DATA);
		
		// convert the input data to a RDD of trajectory objects
		DataConverter rdd = new DataConverter();
		JavaRDD<Trajectory> trajectoryRDD = 
				rdd.mapRawDataToTrajectoryRDD(fileRDD);
		trajectoryRDD.persist(STORAGE_LEVEL);
		
		return trajectoryRDD;
	}
}
