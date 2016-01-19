package uq.truster;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import uq.fs.FileReaderService;
import uq.spark.EnvironmentVariables;
import uq.spatial.GeoInterface;
import uq.spatial.Grid;
import uq.spatial.STRectangle;
import uq.spatial.Trajectory;
import uq.truster.partition.Partition;
import uq.truster.partition.SpatialPartitionModule;
import uq.truster.partition.TrajectoryTrackTable;
import uq.truster.query.QueryProcessingModule;

/**
 * Truster main app class
 * 
 * @author uqdalves
 *
 */
public class TrusterApp implements EnvironmentVariables, GeoInterface {

	/**
	 * Truster main access
	 */
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
		TrajectoryTrackTable trackTable = partitionMod.getTTT();
		
		// force action to build partitions
		System.out.println("Num. Partitions: " + partitionsRDD.count());
		
		/*****
		 * PROCESS QUERIES - TRUSTER QUERY PROCESSING MODULE 
		 *****/
		QueryProcessingModule queryMod = 
				new QueryProcessingModule(partitionsRDD, trackTable, grid);
		// query object
		STRectangle query = new STRectangle(0, 0, 100, 100, 0, 1000);
		// query result
		List<Trajectory> tListResult = 
				queryMod.processSelectionQuery(query);
		System.out.println("Total trajectories returned: " + tListResult.size());
	}
	
	/**
	 * Read input dataset and convert to a RDD of trajectories
	 */
	public static JavaRDD<Trajectory> readData(){
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

	/**
	 * Get min and max coodinate points (x,y) in the dataset
	 * @param trajectoryRDD
	 */
	/*public static double[] getMinMax(JavaRDD<Trajectory> trajectoryRDD){
		double[] minMax = 
			trajectoryRDD.map(new Function<Trajectory, double[]>() {
				public double[] call(Trajectory t) throws Exception {
					double minX = Double.MAX_VALUE;  
					double minY = Double.MAX_VALUE;  
					double maxX = -Double.MAX_VALUE;  
					double maxY = -Double.MAX_VALUE;
					// get min max x and y
					for(Point p : t.getPointsList()){
						if(p.x < minX) minX = p.x;
						if(p.y < minY) minY = p.y;
						if(p.x > maxX) maxX = p.x;
						if(p.y > maxY) maxY = p.y;
					}
					double[] result = new double[]{minX,minY,maxX,maxY};
					return result;
				}
			}).reduce(new Function2<double[], double[], double[]>() {
				public double[] call(double[] v1, double[] v2) throws Exception {
					v1[0] = Math.min(v1[0], v2[0]);
					v1[1] = Math.min(v1[1], v2[1]);
					v1[2] = Math.max(v1[2], v2[2]);
					v1[3] = Math.max(v1[3], v2[3]);
					return v1;
				}
			});
		return minMax;
	}*/
}
