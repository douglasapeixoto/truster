package uq.truster.query;

import java.io.Serializable;

import uq.spatial.Trajectory;

/**
 * A NN is a trajectory with its distance to a 
 * query object. Object to be returned as answer
 * to NN queries.
 *  
 * @author uqdalves
 *
 */
@SuppressWarnings("serial")
public class NearNeighbor extends Trajectory implements Serializable {
	/**
	 * The distance from this NN to the query object.
	 */
	public double distance;

	/**
	 * Initial NN trajectory.
	 */
	public NearNeighbor(Trajectory t) {
		super(t.id);
		this.addPointList(t.getPointsList());
		this.distance = 0.0;
	}
	public NearNeighbor(Trajectory t, double dist) {
		super(t.id);
		this.addPointList(t.getPointsList());
		this.distance = dist;
	}
	public NearNeighbor() {}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		NearNeighbor nn = (NearNeighbor) obj;
		return nn.id.equals(id);
	}
}
