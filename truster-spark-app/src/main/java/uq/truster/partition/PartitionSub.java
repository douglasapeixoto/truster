package uq.truster.partition;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
 
import uq.spatial.Trajectory;
import uq.spatial.TrajectoryRTree;

/**
 * A data partition for sub-trajectories.
 * </br>
 * Each partition represents a rectangle in the 
 * grid representation, and contains a RTree with 
 * the sub-trajectories that overlap with that rectangle.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PartitionSub implements Serializable {
	/**
	 * The tree containing the segments in this partition.
	 */
	private TrajectoryRTree trajectoryTree = 
			new TrajectoryRTree();
	
	/**
	 * The parent trajectory of the sub-trajectories in this partition.
	 */
	private HashSet<String> parentIdSet = 
			new HashSet<String>();
	
	/**
	 * Add a sub-trajectory to this partition.
	 */
	public void add(Trajectory t){
		trajectoryTree.add(t);
		parentIdSet.add(t.id);
	}
	
	/**
	 * The list of sub-trajectories in this partition.
	 */
	public List<Trajectory> getSubTrajectoryList(){
		return trajectoryTree.getTrajectoryList();
	}
	
	/**
	 * Return the set of trajectories that overlaps with this partition.
	 * </br>
	 * The parent trajectories of the sub-trajecoties in this partition.
	 * 
	 * @return Return trajectories ID.
	 */
	public HashSet<String> getTrajectoryIdSet(){
		return parentIdSet;
	}

	/**
	 * Return the set of trajectories that overlaps with this partition,
	 * within the given time interval.
	 * </br>
	 * The parent trajectories of the sub-trajectories in this partition.
	 * 
	 * @return Return trajectories ID.
	 */
	public HashSet<String> getTrajectoryIdSetByTime(long t0, long t1){
		List<Trajectory> list = 
				trajectoryTree.getTrajectoriesByTime(t0, t1);
		HashSet<String> idSet = new HashSet<String>();
		for(Trajectory sub : list){
			idSet.add(sub.id);
		}
		return idSet;
	}
	
	/**
	 * Return the tree of sub-trajectories in this partition.
	 */
	public TrajectoryRTree getSubTrajectoryTree(){
		return trajectoryTree;
	}
	
	/**
	 * Merge two partitions.
	 * 
	 * @return Return this merged partition.
	 */
	public PartitionSub merge(PartitionSub partition){
		trajectoryTree.addAll(partition.getSubTrajectoryList());
		parentIdSet.addAll(partition.getTrajectoryIdSet());
		return this;
	}
	
	/**
	 * Return the number of segments in this partition.
	 */
	public int size(){
		return trajectoryTree.numTrajectories();
	}
	
	/**
	 * Return true is this partitions has no element
	 * or is null.
	 */
	public boolean isEmpty(){
		return trajectoryTree.isEmpty();
	}
}
