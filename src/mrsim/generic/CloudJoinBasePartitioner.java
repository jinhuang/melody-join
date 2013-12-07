package mrsim.generic;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;

import static mrsim.generic.MRSimJoinConfig.*;

public class CloudJoinBasePartitioner  extends Partitioner<CloudJoinKey, VectorElemHD>{
	@Override
	public int getPartition(CloudJoinKey key, VectorElemHD value, int numP) {
		log.setLevel(Level.WARN);

		if (key.getWindowID() != (-1)) {// if it's a window partition, send
			// to window partition
			long min = Math.min(key.getPartitionID(), key.getWindowID());
			long max = Math.max(key.getPartitionID(), key.getWindowID());
			log.debug("PP Key PID: " + key.getPartitionID()
					+ " Partition: "
					+ (int) ((min * 157 + max * 127) % numP));
			return (int) ((min * 157 + max * 127) % numP);
		} else {// if it's it's native partition, send to it's native
			// partition

			log.debug("PW Key PID: " + key.getPartitionID()
					+ " Partition: "
					+ (int) ((key.getPartitionID() * 149) % numP));
			return (int) ((key.getPartitionID() * 149) % numP);
		}
	}

}
