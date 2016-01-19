package uq.truster.partition;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import uq.spatial.STSegment;
import uq.spatial.SegmentRTree;

/**
 * A data partition.
 * Each partition represents a rectangle in the 
 * grid representation, and contains a RTree with 
 * the segments that overlap with that rectangle.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class Partition implements Serializable {
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
	public Partition merge(Partition partition){
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
