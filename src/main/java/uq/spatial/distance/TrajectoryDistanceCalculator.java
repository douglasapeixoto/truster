package uq.spatial.distance;

import uq.spatial.Trajectory;

/**
 * Interface for distance functions between trajectories.
 * 
 * @author uqdalves
 *
 */
public interface TrajectoryDistanceCalculator {
	static final double INFINITY = Double.MAX_VALUE;
	
	/**
	 * Distance between two trajectories.
	 */
	double getDistance(Trajectory t1, Trajectory t2);
}
