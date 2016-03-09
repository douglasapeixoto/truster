package uq.truster.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import uq.spatial.STSegment;
import uq.spatial.TimeComparator;
import uq.spatial.Trajectory;

/**
 * Used in query processing.
 * 
 * Contains a list (bag) of segments satisfying a query Q. 
 * All segments must belong to the same trajectory (parent).
 *  
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class SegmentBag implements Serializable {
	// the segments that composes this query object
	private List<STSegment> segmentsList = 
			new ArrayList<STSegment>();

	/** 
	 * Add a segment to this object.
	 */
	public void add(STSegment t){
		segmentsList.add(t);
	}
	
	/**
	 * The list of segments that compose
	 * this query object. All segments belong 
	 * to the same trajectory (same parent ID).
	 */
	public List<STSegment> getSegmentsList(){
		return segmentsList;
	}

	/**
	 * Merge these two bags.
	 * 
	 * @return Return the merged object.
	 */
	public SegmentBag merge(SegmentBag obj){
		segmentsList.addAll(obj.getSegmentsList());
		return this;
	}
	
	/**
	 * Post-process the segments in this bag.
	 * Merge consecutive segments into trajectories. 
	 * 
	 * @return Return a list of post-processed
	 * trajectories
	 */
	public List<Trajectory> postProcess(){
		List<Trajectory> tListAux = new ArrayList<Trajectory>();
		if(segmentsList.size() == 0){
			return tListAux;
		}
		String parentId = segmentsList.get(0).parentId;
		if(segmentsList.size() == 1){
			Trajectory t = new Trajectory(parentId);
			t.addSegment(segmentsList.get(0));
			tListAux.add(t);
			return tListAux;
		}
		
		// sort the segments by initial time-stamp
		TimeComparator<STSegment> comparator = 
				new TimeComparator<STSegment>();
		Collections.sort(segmentsList, comparator);

		// add 1st segment
		Trajectory sub = new Trajectory(parentId);
		sub.addSegment(segmentsList.get(0));
		for(int i=1; i<segmentsList.size(); i++){
			STSegment s = segmentsList.get(i);
			if(sub.last().equals(s.p1())){
				sub.addSegment(s);
			}else{
				tListAux.add(sub);
				sub = new Trajectory(parentId);
				sub.addSegment(s);
			}
		}
		// add the last sub-trajectory
		tListAux.add(sub);
		
		// remove duplicate points from each sub-trajectory
		List<Trajectory> tList = new ArrayList<Trajectory>();
		for(Trajectory aux : tListAux){
			aux.removeDuplicates();
			tList.add(aux);
		}

		return tList;
	}
}
