package uq.truster.partition;

import java.io.Serializable;
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
	 * Add a segment to this partition.
	 */
	public void add(STSegment s){
		segmentTree.add(s);
	}
	
	/**
	 * The list of segments in this partition.
	 */
	public List<STSegment> getSegmentsList(){
		return segmentTree.getSegmentsList();
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
