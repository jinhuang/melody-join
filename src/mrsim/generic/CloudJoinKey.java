package mrsim.generic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class CloudJoinKey  implements WritableComparable<CloudJoinKey> {

		
		long partitionID		= -1;
		long windowID			= -1;
		long prevItrPartion	 	= -1;
		
		CloudJoinKey()
		{
			System.out.println("Why am I seeing this?");
		}
		
		public CloudJoinKey(long partitionID)
		{
			this.partitionID = partitionID;
		}
		
		CloudJoinKey(long partitionID, long windowID)
		{
			this.partitionID = partitionID;
			this.windowID = windowID;
		}

		public CloudJoinKey(long partitionID, long windowID, long prevItrPartion)
		{
			this.partitionID = partitionID;
			this.windowID = windowID;
			this.prevItrPartion = prevItrPartion;
		}		
		
	public long getPartitionID() {
		return partitionID;
	}

	public void setPartitionID(long partitionID) {
		this.partitionID = partitionID;
	}

	public long getWindowID() {
		return windowID;
	}

	public void setWindowID(long windowID) {
		this.windowID = windowID;
	}

	public long getPrevItrPartion() {
		return prevItrPartion;
	}

	public void setPrevItrPartion(long prevItrPartion) {
		this.prevItrPartion = prevItrPartion;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		partitionID = in.readLong();
		windowID = in.readLong();
		prevItrPartion = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(partitionID);
		out.writeLong(windowID);
		out.writeLong(prevItrPartion);
	}

	@Override
	public int compareTo(CloudJoinKey o) 
	{
		if(this.partitionID != o.getPartitionID())
			return this.partitionID < o.getPartitionID() ? -1 : 1;
		else if(this.windowID != o.getWindowID())
			return this.windowID < o.getWindowID() ? -1 : 1;
		else if(this.prevItrPartion != o.getPrevItrPartion())
			return this.prevItrPartion < o.getPrevItrPartion() ? -1 : 1;
		else
			return 0;
	}
	
	public String toString()
	{
		return "PID: " + partitionID + " WID: " + windowID + " PIP: " + prevItrPartion;	
	}
	
}
