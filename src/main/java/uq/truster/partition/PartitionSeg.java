package uq.truster.partition;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import uq.spatial.STSegment;
import uq.spatial.SegmentRTree;

/**
 * A data partition for segments.
 * </br>
 * Each partition represents a rectangle in the 
 * grid representation, and contains a RTree with 
 * the segments that overlap with that rectangle.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class PartitionSeg implements Serializable {
	/**
	 * The tree containing the segments in this partition.
	 */
	private SegmentRTree segmentTree = 
			new SegmentRTree();

	/**
	 * The parent trajectory of the segments in this partition.
	 */
	private HashSet<String> parentIdSet = 
			new HashSet<String>();
	
	/**
	 * Add a segment to this partition.
	 */
	public void add(STSegment s){
		segmentTree.add(s);
		parentIdSet.add(s.parentId);
	}
	
	/**
	 * The list of segments in this partition.
	 */
	public List<STSegment> getSegmentsList(){
		return segmentTree.getSegmentsList();
	}
	
	/**
	 * Return the set of trajectories that overlaps with this partition.
	 * </br>
	 * The parent trajectories of the segments in this partition.
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
	 * The parent trajectories of the segments in this partition.
	 * 
	 * @return Return trajectories ID.
	 */
	public HashSet<String> getTrajectoryIdSetByTime(long t0, long t1){
		List<STSegment> list = 
				segmentTree.getSegmentsByTime(t0, t1);
		HashSet<String> idSet = new HashSet<String>();
		for(STSegment s : list){
			idSet.add(s.parentId);
		}
		return idSet;
	}
	
	/**
	 * Return the tree of segments in this partition.
	 */
	public SegmentRTree getSegmentsTree(){
		return segmentTree;
	}
	
	/**
	 * Merge two partitions.
	 * 
	 * @return Return this merged partition.
	 */
	public PartitionSeg merge(PartitionSeg partition){
		segmentTree.addAll(partition.getSegmentsList());
		parentIdSet.addAll(partition.getTrajectoryIdSet());
		return this;
	}
	
	/**
	 * Return the number of segments in this partition.
	 */
	public int size(){
		return segmentTree.numSegments();
	}
	
	/**
	 * Return true is this partitions has no element
	 * or is null.
	 */
	public boolean isEmpty(){
		return segmentTree.isEmpty();
	}
}
