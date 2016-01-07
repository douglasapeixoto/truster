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
		List<Trajectory> tList = new ArrayList<Trajectory>();
		if(segmentsList.size() == 0){
			return tList;
		}
		String parentId = segmentsList.get(0).parentId;
		if(segmentsList.size() == 1){
			Trajectory t = new Trajectory(parentId);
			t.addSegment(segmentsList.get(0));
			tList.add(t);
			return tList;
		}
		
		// sort the sub-trajectories by initial time-stamp
		TimeComparator<STSegment> comparator = 
				new TimeComparator<STSegment>();
		Collections.sort(segmentsList, comparator);
		
		// merge consecutive sub-trajectories
		Trajectory sub_0 = new Trajectory(parentId);
		sub_0.addSegment(segmentsList.get(0));
		for(int i=1; i<segmentsList.size(); i++){
			Trajectory sub_i = new Trajectory(parentId);
			sub_i.addSegment(segmentsList.get(i));
			// bug do sort fix
			if(sub_0.head().equals(sub_i.head())){
				sub_0 = sub_0.size() > sub_i.size() ? sub_0 : sub_i;
			} else {
				// check last segment of sub_0 with first of sub_i
				if(sub_i.size() > 1){
					// merge the segments
					if(sub_0.tail().equals(sub_i.head())){
						sub_0.merge(sub_i.subTrajectory(1, sub_i.size()));
					}
					else if(sub_0.tail().equals(sub_i.get(1))){
						sub_0.merge(sub_i.subTrajectory(2, sub_i.size()));
					} else{
						tList.add(sub_0);
						sub_0 = new Trajectory(parentId);
						sub_0.addSegment(segmentsList.get(i));
					}
				} else if(sub_i.size() == 1){
					if(!sub_0.tail().equals(sub_i.head())){
						tList.add(sub_0);
						sub_0 = new Trajectory(parentId);
						sub_0.addSegment(segmentsList.get(i));
					}
				}
			}
		}
		// add final sub-trajectory
		tList.add(sub_0);

		return tList;
	}
}
