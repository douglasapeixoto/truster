package uq.truster;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import uq.fs.FileReader;
import uq.spark.EnvironmentVariables;
import uq.spatial.GeoInterface;
import uq.spatial.Grid;
import uq.spatial.Point;
import uq.spatial.STRectangle;
import uq.spatial.STSegment;
import uq.spatial.Trajectory;
import uq.truster.partition.PartitionSeg;
import uq.truster.partition.SpatialPartitionModuleSeg;
import uq.truster.partition.TrajectoryTrackTableSeg;
import uq.truster.query.NearNeighbor;
import uq.truster.query.QueryProcessingModuleSeg;

/**
 * General test
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Test implements Serializable, EnvironmentVariables, GeoInterface{
	// service to read files
	private static final FileReader reader = 
			new FileReader();
	
	/**
	 * Main: Benchmark comparison testing.
	 */
	public static void main(String[] arg0){
		System.out.println("\n[TRUSTER] Application Starts..\n");
		
		// READ DATA
		JavaRDD<Trajectory> trajectoryRDD = reader.readData();
		// create grid
		Grid grid = new Grid(SIZE_X, SIZE_Y, MIN_X, MIN_Y, MAX_X, MAX_Y);
		grid.print();
		
		// PARTITIONING
		SpatialPartitionModuleSeg partitionMod = new SpatialPartitionModuleSeg();
		JavaPairRDD<Integer, PartitionSeg> partitionsRDD = 
				partitionMod.partition(trajectoryRDD, grid);
		System.out.println("Number of partitions: " + partitionsRDD.count());
		// print partitions RDD
		partitionsRDD.foreach(new VoidFunction<Tuple2<Integer,PartitionSeg>>() {
			public void call(Tuple2<Integer, PartitionSeg> partition) throws Exception {
				System.out.println("Partition: " + partition._1);
				for(STSegment seg : partition._2.getSegmentsList()){
					System.out.println(seg.toString());
				}
				System.out.println();
			}
		});
		
		// print track table
		TrajectoryTrackTableSeg trackTable = partitionMod.getTTT();		
		trackTable.print();
		
		// QUERY PROCESSING
		QueryProcessingModuleSeg queryModule = 
				new QueryProcessingModuleSeg(partitionsRDD, trackTable, grid);
		
		// SELECTION QUERY
		STRectangle r1 = new STRectangle(0, 0, 10, 10, 0, 10);
		STRectangle r2 = new STRectangle(0, 0, 30, 20, 0, 10);
		STRectangle r3 = new STRectangle(12, 22, 28, 28, 0, 10);
		
		List<Trajectory> result = 
				queryModule.processSelectionQuery(r1);
		System.out.println("Query 1 Result:");
		for(Trajectory t : result){
			t.print();
		}
		System.out.println();
		result =
				queryModule.processSelectionQuery(r2);
		System.out.println("Query 2 Result:");
		for(Trajectory t : result){
			t.print();
		}
		System.out.println();
		result =
				queryModule.processSelectionQuery(r3);
		System.out.println("Query 3 Result:");
		for(Trajectory t : result){
			t.print();
		}
		System.out.println();
		
		// K-NN QUERY
		Trajectory q = new Trajectory("Q");
		Point p1 = new Point(15, 27, 0);
		Point p2 = new Point(18, 27, 1);
		Point p3 = new Point(22, 27, 2);
		Point p4 = new Point(25, 27, 3);
		q.addPoint(p1);
		q.addPoint(p2);
		q.addPoint(p3);
		q.addPoint(p4);
		
		List<NearNeighbor> nnResult = 
				queryModule.processKNNQuery(q, 0, 10, 2);
		System.out.println("2-NN Query Result:");
		for(Trajectory t : nnResult){
			t.print();
		}
		System.out.println();

		System.out.println("\n[TRUSTER] Application Ends..\n");
	}
}
