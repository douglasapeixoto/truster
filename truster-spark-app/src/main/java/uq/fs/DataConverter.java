package uq.fs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import uq.spark.EnvironmentVariables;
import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
 * Service to convert input files (raw data) to RDDs 
 * of objects (e.g. Trajectory, Points, etc.).
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class DataConverter implements Serializable, EnvironmentVariables{

	/**
	 * Convert an input dataset (RDD) file of trajectories
	 * to point objects.
	 * 
	 * @return Return a RDD of points.
	 */
	public JavaRDD<Point> mapRawDataToPointRDD(JavaRDD<String> fileRDD){
		// Map to read the file and convert each line to trajectory points objects
     	JavaRDD<Point> pointsRDD = 
     			fileRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Point>() {

			public Iterable<Point> call(Iterator<String> partition) throws Exception {
				// A list of points to return	
				List<Point> pointList = 
						  new ArrayList<Point>();
		
				// read each line of the file of this partition
				// each line is one trajectory
				while(partition.hasNext()){
					// read the line
					String line = partition.next();
			
					if(line.length() > 3){
						// read attributes
						String[] tokens = line.split(" "); 

						// read sample points (token[0] is the id)
						double x, y;
						long time;
						for(int i=1; i<tokens.length; i+=3){
							x = Double.parseDouble(tokens[i]);
							y = Double.parseDouble(tokens[i+1]);
							time = Long.parseLong(tokens[i+2]);
							
							pointList.add(new Point(x, y, time));
						}
					}
				}
				return pointList;
			}
		});
     	
     	return pointsRDD;
	}
	
	/**
	 *  Convert an input dataset (RDD) file 
	 *  to trajectory objects.
	 * 
	 * @return Return a RDD of trajectories.
	 */
	public JavaRDD<Trajectory> mapRawDataToTrajectoryRDD(JavaRDD<String> fileRDD){
		// Map to read the file and convert each line to trajectory objects
     	JavaRDD<Trajectory> trajectoriesRDD = 
     			fileRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Trajectory>() {

			public Iterable<Trajectory> call(Iterator<String> partition) throws Exception {
				// A list of trajectories to return	
				List<Trajectory> trajectoryList = 
						  new ArrayList<Trajectory>();

				// read each line of the file of this partition.
				// each line is one trajectory
				while(partition.hasNext()){
					// read the line
					String line = partition.next();
			
					if(line.length() > 3){ // ensure no empty trajectories
						// read attributes
						String[] tokens = line.split(" ");

						// new trajectory for this line
						Trajectory t = new Trajectory(tokens[0]);
						
						// read sample points
						double x, y;
						long time;
						for(int i=1; i<tokens.length; i+=3){
							x = Double.parseDouble(tokens[i]);
							y = Double.parseDouble(tokens[i+1]);
							time = Long.parseLong(tokens[i+2]);
							
							t.addPoint(new Point(x, y, time));
						}
						trajectoryList.add(t);
					}
				}
				return trajectoryList;
			}
		});
     	
     	return trajectoriesRDD;
	}
	
	/**
	 * Read the dataset and select a given number of 
	 * sample points. Save to HDFS if required, save
	 * as 'sample-points-$num'.

	 * @return A list with num points
	 */
	public List<Point> selectSamplePoints(
			JavaRDD<String> fileRDD, 
			final int num, boolean save){
		System.out.println("\nSelecting " + num + " Sample Points..");
     	System.out.println();
     	
		// map the input dataset to point objects
	 	JavaRDD<Point> pointsRDD = mapRawDataToPointRDD(fileRDD);
	 	// select sample
	 	List<Point> sampleList = pointsRDD.takeSample(false, num);

	 	// save to HDFS
	 	if(save){
	 		HDFSFileService hdfs = new HDFSFileService();
	 		hdfs.savePointListHDFS(sampleList, "sample-points-"+ num);
	 	}

	 	return sampleList;
	}	
	
	/**
	 * Read the dataset and select a given number of 
	 * sample trajectories. Save to HDFS if required,
	 * save as 'sample-trajectories-$num'.

	 * @return A list with num trajectories.
	 */
	public List<Trajectory> selectSampleTrajectories(
			JavaRDD<String> fileRDD, 
			final int num, boolean save){
	 	System.out.println("\nSelecting " + num + " Sample Trajectories..");
     	System.out.println();

     	// map the input dataset to point objects
 	 	JavaRDD<Trajectory> trajectoryRDD = mapRawDataToTrajectoryRDD(fileRDD);
 	 	// select sample
 		List<Trajectory> sampleList = trajectoryRDD.takeSample(false, num);
 	 	
 	 	// save to HDFS
 		if(save){
 			HDFSFileService hdfs = new HDFSFileService();
	 		hdfs.saveTrajectoryListHDFS(sampleList, "sample-trajectories-"+ num);
	 	}
 		
 		return sampleList;
	}

}
