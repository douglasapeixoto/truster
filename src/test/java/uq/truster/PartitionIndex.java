package uq.truster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("serial")
public class PartitionIndex implements WritableComparable<PartitionIndex>, Serializable{
	public int x;
	public int y;
	
	public PartitionIndex(){}
	public PartitionIndex(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
        out.writeInt(y);
	}
	
	public int compareTo(PartitionIndex obj) {		
		return x == obj.x ? (y - obj.y) : (y - obj.y);
	}

	@Override
	public String toString() {
		return (x + "," + y);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + x;
		result = prime * result + y;
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PartitionIndex other = (PartitionIndex) obj;
		if (x != other.x)
			return false;
		if (y != other.y)
			return false;
		return true;
	}

}
