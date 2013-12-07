package mrsim.generic;

import static com.iojin.melody.utils.FileUtil.getData;
import static mrsim.generic.MRSimJoinConfig.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;



public class CloudJoinWindowMapper  extends Mapper<LongWritable, Text, CloudJoinKey, VectorElemHD>{
	ArrayList<VectorElemHD> pivPts;
	double [] bins;
	int dimension=1;
	@Override
	protected void setup(Context context) {
		log.setLevel(Level.WARN);
		log.debug("WM we have started configure?");
		Configuration conf  = context.getConfiguration();
		dimension = Integer.parseInt(conf.get("dimension"));
		int binsLength = Integer.parseInt(conf.get("binsLength"));
		String binsPath = conf.get("binsPath");
		String binsName = binsPath.substring(binsPath.lastIndexOf("/")+1,binsPath.length());
		
		try {
			bins = new double[binsLength];
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			log.debug("BM cacheFiles length: " + cacheFiles.length);
			if (null != cacheFiles && cacheFiles.length>0) {
				for(Path path:cacheFiles){
					if(path.toString().indexOf(binsName)>-1){
						bins = getData(path.toString(),bins.length);	
					}
					if(path.toString().indexOf("pivots.txt")>-1){
						loadPivPts(path);
					}
				}
				
			} else {
				log.debug("BM Error accessing distributed cache");
			}
		} catch (IOException ioe) {
			log.debug("WM IOException reading from distributed cache");
			log.debug(ioe.toString());
		}
	}

	void loadPivPts(Path cachePath) throws IOException {
		// note use of regular java.io methods here - this is a local file
		// now
		BufferedReader lineReader = new BufferedReader(new FileReader(
				cachePath.toString()));
		try {
			String line;
			this.pivPts = new ArrayList<VectorElemHD>();
			while ((line = lineReader.readLine()) != null) {
				// parse the line into a StringElem object
				this.pivPts.add(new VectorElemHD(line));
			}

		} finally {
			lineReader.close();
		}
	}

	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		log.debug("===================================Window Map Begin===================================");
		String s;
		s = context.getConfiguration().get("eps", "");
		if ("".equals(s)) {
			System.out
					.println("Error: Unable to get eps from configuration");
		}
		double eps = Double.parseDouble(s);
		s = context.getConfiguration().get("W", "");
		if ("".equals(s)) {
			System.out
					.println("Error: Unable to get memory from configuration");
		}
//		long w = Long.parseLong(s);
		s = context.getConfiguration().get("V", "");
		if ("".equals(s)) {
			System.out
					.println("Error: Unable to get itr from configuration");
		}
//		long v = Long.parseLong(s);

		VectorElemHD elem;
		try {
			elem = new VectorElemHD(inputValue); // creates element that is
			// being worked on in
			// this map
		} catch (Exception e) {
			log.debug(inputValue.toString());
			return;
		}

		VectorElemHD pivot_curr; // tracks the current pivot point
		VectorElemHD pivot_best; // current best pivot point/partition
		double min_distance; // current min distance between elem and
		// pivot_best

		Iterator<VectorElemHD> pivItr = pivPts.iterator();
		if (pivItr.hasNext()) {
			pivot_best = pivItr.next(); // establish a first pivot point
			min_distance = elem.getDistanceByEmd(pivot_best, bins,dimension);
		} else {
			System.out.println("Error: Distributed cache is empty"); // error
			// if
			// none
			// found
			return;
		}
		while (pivItr.hasNext()) // check all partitions point and find best
		// match
		{
			pivot_curr = pivItr.next();
			double curr_distance = elem.getDistanceByEmd(pivot_curr, bins,dimension);
			if (curr_distance < min_distance) // if the current partition is
			// closer than the current
			// best
			{
				pivot_best = pivot_curr; // update pivot_best
				min_distance = curr_distance; // update current distance
			}
		}

		// creates a key for writing to the context
		CloudJoinKey cjk = new CloudJoinKey(pivot_best.getKey(), -1, elem
				.getPrevPartition());
		elem.setPartitionID(pivot_best.getKey()); // set element native
		// partition (may not be
		// needed)
		context.write(cjk, elem); // writes to context the key value pair

		String dbg = "WM----------------------------------------------------------------------" + '\n';
		dbg += "WM Elem: " + elem.toStringPart() + '\n';
		dbg += "WM CJK: " + cjk.toString() + '\n';
		dbg += "WM----------------------------------------------------------------------" + '\n';
		log.debug(dbg);

		pivItr = pivPts.iterator(); // check if the elem is in a window of
		// any of the other partitions
		while (pivItr.hasNext()) {
			pivot_curr = pivItr.next(); // get a current working pivot
			if (pivot_curr.getKey() == pivot_best.getKey()) {
				continue; // if the current pivot is the same as the best
				// match,
				// continue onto next pivot
			}
			double curr_distance = elem.getDistanceByEmd(pivot_curr, bins,dimension);// get
			// the
			// distance
			// between
			// the
			// current
			// pivot
			// and
			// the
			// elem

			if (((curr_distance - min_distance) / 2) <= eps) // check if
			// it's in
			// in the
			// range of
			// the
			// window
			{
				// add to window partition
				cjk.setWindowID(pivot_curr.getKey()); // set the window id
				// on the keyto the
				// key key of the
				// current pivot
				context.write(cjk, elem);

				dbg = "WM window----------------------------------------------------------------------" + '\n';
				dbg += "WM window Elem: " + elem.toStringPart() + '\n';
				dbg += "WM window CJK: " + cjk.toString() + '\n';
				dbg += "WM window----------------------------------------------------------------------" + '\n';
				log.debug(dbg);
			}
		}
		log
				.debug("===================================Window Map End===================================");
	}// end map

}
