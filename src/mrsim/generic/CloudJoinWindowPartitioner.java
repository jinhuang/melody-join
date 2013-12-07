package mrsim.generic;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class CloudJoinWindowPartitioner extends Partitioner<CloudJoinKey, VectorElemHD> implements Configurable{
	private Configuration conf;

	public int getPartition(CloudJoinKey key, VectorElemHD value, int numP) {

		// we should perform a check here and make sure we get values back
		long W = Long.parseLong(conf.get("W"));
		long V = Long.parseLong(conf.get("V"));

		if (key.getWindowID() == -1) {
			return (int) ((key.getPartitionID() * 163) % numP);
		} else {
			long min = Math.min(key.getPartitionID(), key.getWindowID());
			long max = Math.max(key.getPartitionID(), key.getWindowID());

			if (((key.getPartitionID() < key.getWindowID()) && (key
					.getPrevItrPartion() == W))
					|| ((key.getPartitionID() > key.getWindowID()) && (key
							.getPrevItrPartion() == V))) {
				return (int) ((min * 179 + max * 197) % numP);
			} else {
				return (int) ((min * 173 + max * 181) % numP);
			}
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
