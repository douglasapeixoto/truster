package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import gnu.trove.procedure.TIntProcedure;
import net.sf.jsi.Rectangle;
import net.sf.jsi.rtree.RTree;


/**
 * A serializable 1D RTree of segments time-stamp.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SegmentRTree extends RTree implements Serializable, GeoInterface{
	// the list of trajectories in this tree
	private List<STSegment> segmentList = 
			new ArrayList<STSegment>();
	// null dimension
	private static final float NULL_DIM_MIN = 0;
	private static final float NULL_DIM_MAX = 1;
	
	/**
	 * Initialize this tree
	 */
	public SegmentRTree() {
		super();
		super.init(null);
	}

	/**
	 * True is this tree is empty or null.
	 */
	public boolean isEmpty(){
		return (this.size()==0);
	}
	
	/**
	 * Add a segment to this tree. 
	 */
	public void add(STSegment s){
		// add the time-stamp of this segment to the RTree
		// add as a rectangle of base=time-stamp and height=NULL_DIM
		Rectangle r = new Rectangle(s.t1, NULL_DIM_MIN, s.t2, NULL_DIM_MAX);
		super.add(r, segmentList.size());
		this.segmentList.add(s);
	}
	
	/**
	 * Add a list of segments to this tree. 
	 */
	public void addAll(List<STSegment> sList){
		for(STSegment s : sList){
			this.add(s);
		}
	}

	/**
	 * Given a time interval, return the segments in this
	 * tree whose time stamp overlap with the given time interval.
	 */
	public List<STSegment> getSegmentsByTime(final long t1, final long t2){
		final List<STSegment> sList = 
				new ArrayList<STSegment>();
		if(isEmpty()){
			return sList;
		}
		final Rectangle r = new Rectangle(t1, NULL_DIM_MIN, t2, NULL_DIM_MAX);
		this.intersects(r, new TIntProcedure() {
			public boolean execute(int i) {
				sList.add(segmentList.get(i));	
				return true;  // continue receiving results
			}
		});
		return sList;
	}

	/**
	 * Return a list with all segments in this tree.
	 */
	public List<STSegment> getSegmentsList() {
		return segmentList;
	}
	
	/**
	 * The number of segments in this tree.
	 */
	public int numSegments(){
		return segmentList.size();
	}
	
	/**
	 * Merge these two segment trees.
	 * 
	 * @return Return this merged tree.
	 */
	public SegmentRTree merge(SegmentRTree tree){
		for(STSegment s : tree.segmentList){
			this.add(s);
		}
		return this;
	}
}
