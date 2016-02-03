package uq.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import gnu.trove.procedure.TIntProcedure;
import net.sf.jsi.Rectangle;
import net.sf.jsi.rtree.RTree;


/**
 * A serializable 1D RTree of trajectories time-stamp.
 * 
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class TrajectoryRTree extends RTree implements Serializable, GeoInterface{
	// the list of trajectories in this tree
	private List<Trajectory> trajectoryList = 
			new ArrayList<Trajectory>();
	// null dimension
	private static final float NULL_DIM_MIN = 0;
	private static final float NULL_DIM_MAX = 1;
	
	/**
	 * Initialize this tree
	 */
	public TrajectoryRTree() {
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
	 * Add a trajectory to this tree. 
	 */
	public void add(Trajectory t){
		// add the time-stamp of this sub-trajectory to the RTree
		// add as a rectangle of base=time-stamp and height=NULL_DIM
		Rectangle r = new Rectangle(t.timeIni(), NULL_DIM_MIN, t.timeEnd(), NULL_DIM_MAX);
		super.add(r, trajectoryList.size());
		this.trajectoryList.add(t);
	}
	
	/**
	 * Add a list of trajectories to this tree. 
	 */
	public void addAll(List<Trajectory> tList){
		// add the MBR of each trajectory to the RTree
		for(Trajectory t : tList){
			this.add(t);
		}
	}
	
	/**
	 * Given a time interval, return the sub-trajectories in this
	 * tree whose time stamp overlap with the given time interval.
	 */
	public List<Trajectory> getTrajectoriesByTime(final long t1, final long t2){
		final List<Trajectory> tList = 
				new ArrayList<Trajectory>();
		if(isEmpty()){
			return tList;
		}
		final Rectangle r = new Rectangle(t1, NULL_DIM_MIN, t2, NULL_DIM_MAX);
		this.intersects(r, new TIntProcedure() {
			public boolean execute(int i) {
				tList.add(trajectoryList.get(i));	
				return true;  // continue receiving results
			}
		});
		return tList;
	}
	
	/**
	 * Return a list with all trajectories in this tree.
	 */
	public List<Trajectory> getTrajectoryList() {
		return trajectoryList;
	}
	
	/**
	 * The number of trajectories in this tree.
	 */
	public int numTrajectories(){
		return trajectoryList.size();
	}
	
	/**
	 * Merge these two trajectory trees.
	 * 
	 * @return Return this merged tree.
	 */
	public TrajectoryRTree merge(TrajectoryRTree tree){
		for(Trajectory t : tree.trajectoryList){
			this.add(t);
		}
		return this;
	}
}
