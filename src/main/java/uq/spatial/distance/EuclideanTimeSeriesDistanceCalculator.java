package uq.spatial.distance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uq.spatial.Point;
import uq.spatial.Trajectory;

/**
* Euclidean distance for multidimensional time series. 
*
* @author uqhsu1, uqdalves
*/
@SuppressWarnings("serial")
public class EuclideanTimeSeriesDistanceCalculator implements Serializable, TrajectoryDistanceCalculator {

	@Override
	public double getDistance(Trajectory t1, Trajectory t2){
		// make sure the original trajectories will not be changed
		List<Point> r_clone = t1.clone().getPointsList();
		List<Point> s_clone = t2.clone().getPointsList();
		
		// get distance
		double dist = getEDC(r_clone, s_clone); 
		if(Double.isNaN(dist)){
			return INFINITY;
		}
		return dist;
	}

	private double getEDC(List<Point> r, List<Point> s)
	{
		List<Point> longT = new ArrayList<Point>();
		List<Point> shortT = new ArrayList<Point>();

		if (r.size() == 0 && s.size() == 0)
		{
			return 0;
		}
		if (r.size() == 0 || s.size() == 0)
		{
			return Double.MAX_VALUE;
		}

		if (r.size() < s.size())
		{
			shortT = r;
			longT = s;
		}
		else
		{
			shortT = s;
			longT = r;
		}
		int k = shortT.size();

		double[] distanceOption = new double[longT.size() - shortT.size() + 1];

		for (int i = 0; i < distanceOption.length; i++)
		{
			double tempResult = 0;
			for (int j = 0; j < k; j++)
			{
				tempResult += shortT.get(j).dist(longT.get(j + i));
			}
			tempResult /= k;
			distanceOption[i] = tempResult;
		}

		return GetMin(distanceOption);

	}

	private double GetMin(double[] a)
	{
		assert (a.length > 0);

		double result = a[0];

		for (int i = 0; i < a.length; i++)
		{
			if (result < a[i])
			{
				result = a[i];
			}
		}

		return result;
	}

	public String toString()
	{
		return "ED with MultiDimension";
	}

}
