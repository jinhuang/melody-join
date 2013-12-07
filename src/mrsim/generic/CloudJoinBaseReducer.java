package mrsim.generic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;

import com.iojin.melody.utils.DistanceType;
import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.DualBound;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.ReductionBound;


import static com.iojin.melody.utils.FileUtil.getData;
import static mrsim.generic.MRSimJoinConfig.*;

public class CloudJoinBaseReducer extends Reducer<CloudJoinKey, VectorElemHD, Text, Text>{

	double [] bins;
	double [] vector;
	double [] eachProjection;
	double [] projectedBins;
	int numVector;
	int dimension=1;
	int numBins;
	
	private int dualCounter = 0;
	private static final int numDual = 10;
	private static DualBound[] duals = new DualBound[numDual];
	private static final int numReduced = 10;
	private static final int reducedDimension = 8;
	private static ReductionBound[] reductions = new ReductionBound[numReduced];	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf  = context.getConfiguration();
		dimension = Integer.parseInt(conf.get("dimension"));
		int binsLength = Integer.parseInt(conf.get("binsLength"));
		String binsPath = conf.get("binsPath");
		String binsName = binsPath.substring(binsPath.lastIndexOf("/")+1,binsPath.length());
		
		bins = new double[binsLength];
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (null != cacheFiles && cacheFiles.length>0) {
			for(Path path:cacheFiles){
				if(path.toString().indexOf(binsName)>-1){
					bins = getData(path.toString(),bins.length);	
				}				
			}
			
		} 
		String vectorPath = conf.get("vectorPath");
		String vectorName = FileUtil.getNameFromPath(vectorPath);
		numVector = conf.getInt("numVector", 0);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, numVector * dimension);
		numBins = binsLength / dimension;
		eachProjection = new double[numBins];
		projectedBins = HistUtil.projectBins(bins, dimension, vector, numVector);
		
		for (int i = 0; i < numReduced; i++) {
			reductions[i] = new ReductionBound(dimension, reducedDimension, bins);
		}		
	}

	public void reduce(CloudJoinKey key, Iterable<VectorElemHD> values,
			Context context) {
		log.setLevel(Level.WARN);
		log.debug("===================================Base Reduce Begin===================================");		
		String s;
		s = context.getConfiguration().get("eps", "");
		if ("".equals(s)) {
			log.error("BR Error: Unable to get eps from configuration");
		}
		double eps = Double.parseDouble(s);
		s = context.getConfiguration().get("memory", "");
		if ("".equals(s)) {
			log.error("BR Error: Unable to get memory from configuration");
		}
		long memory = Long.parseLong(s);
		s = context.getConfiguration().get("itr", "");
		if ("".equals(s)) {
			log.error("BR Error: Unable to get itr from configuration");
		}
		long itr = Long.parseLong(s);
		String outDir = context.getConfiguration().get("outDir", "");
		if ("".equals(outDir)) {
			log.error("BR Error: Unable to get outDir from configuration");
		}
		long sizeOfElem = -1;
		boolean performInMemoryJoin = true;
		ArrayList<VectorElemHD> inMemoryList = new ArrayList<VectorElemHD>();

		log.debug("BR start inmemory collection");
		log.debug("BR CJK: " + key.toString());
		Iterator<VectorElemHD> valItr = values.iterator();
		int counter = 0;
		while (valItr.hasNext()) {
			VectorElemHD currElem = copyVectorElem(valItr.next());
			if (sizeOfElem == -1)
				sizeOfElem = currElem.getSizeInBytes();

			inMemoryList.add(counter, currElem);
			counter++;

			// if we have reached the limit of memory and there is more to
			// do, break and don't perform inMemoryJoin
			
			/*
			 * the inMemoryList's size decide whether invoke the Window-pair Partition
			 * at the same time, the inMemoryList's size is related with the quantity of pivots. The more pivots the small size.
			 * 
			 * */
							
			
			if ((memory <= ((inMemoryList.size()) * sizeOfElem))
					&& valItr.hasNext()) {
				log.debug("BR we are not performing in memory join");
				performInMemoryJoin = false;
				break;
			}
		}

		if (performInMemoryJoin) {
			log.debug("BR performInMemoryJoin - Base");
			// for all elements in inMemoryList,
			// check if they can be joined to all others
			// if they can, write output to final output
			inMemoryList.trimToSize();
			baseQuickJoin(inMemoryList.subList(0, inMemoryList.size()),eps, context, key.getWindowID());
			
			context.progress();

		}// end in memory join
		else// perform out of memory operations
		{
			FileSystem fs; // create variable for file system access
			Path outfile;
			Path itrOutDir;

			FSDataOutputStream[] out = new FSDataOutputStream[numFilesToGen];

			log.debug("BR performOutOfMemoryOperation");

			counter = 0;

			if (key.getWindowID() == -1) // it's a normal partition
			{
				try {
					fs = FileSystem.get(context.getConfiguration()); // try to set up a output stream to the HDFS

					itrOutDir = new Path(outDir + "intermediate_output_"+ itr + "_" + key.getPartitionID());
					fs.mkdirs(itrOutDir);
					log.debug("++++++Trying to make dir where WID = -1 :"+itrOutDir);
					
					for (int i = 0; i < numFilesToGen; i++) {
						outfile = new Path(itrOutDir.toString() + "/" + itr+ "_" + i + ".txt");
						out[i] = fs.create(outfile);
					}
					log.debug("Output Dir: " + itrOutDir.toString());
				} catch (IOException e) {
					log.debug("BR WindowID = -1: Error creating directory/file");
					log.debug(e.getMessage()); // error: unable to create
					// stream
					e.printStackTrace();
					return;
				}

				while (valItr.hasNext()) {
					VectorElemHD currElem = valItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem.toString() + '\n');
						counter++;
					} catch (IOException e) {
						log.debug("BR WindowID = -1: Error writing remaining elements in que");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}

				Iterator<VectorElemHD> inMemoryItr = inMemoryList	.iterator();
				while (inMemoryItr.hasNext()) {
					VectorElemHD currElem = inMemoryItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem
								.toString() + '\n');
						counter++;
					} catch (IOException e) {
						log.debug("BR WindowID = -1: Error writing inMemoryList elements");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}
				for (int i = 0; i < out.length; i++)
					try {
						out[i].close();
					} catch (IOException e) {
						log.debug("BR WindowID != -1: Error closing outfiles");
						e.printStackTrace();
					}
			}// end it's a normal partion
			else // it's a window partition
			{
				long min = Math.min(key.getPartitionID(), key.getWindowID());
				long max = Math.max(key.getPartitionID(), key.getWindowID());
				try {
					fs = FileSystem.get(context.getConfiguration()); // try to set up a output stream to the HDFS
					itrOutDir = new Path(outDir + "intermediate_output_"+ itr + "_" + min + "_" + max);
					fs.mkdirs(itrOutDir);
					log.debug("---------Trying to make dir on WP:"+itrOutDir);
					
					for (int i = 0; i < numFilesToGen; i++) {
						outfile = new Path(itrOutDir.toString() + "/" + itr
								+ "_" + i + ".txt");
						out[i] = fs.create(outfile);
					}
					log.debug("Output Dir: " + itrOutDir.toString());
				} catch (IOException e) {
					log.debug("BR WindowID != -1: Error creating directory/file");
					log.debug(e.getMessage());
					e.printStackTrace(); // error: unable to create stream
					return;
				}

				while (valItr.hasNext()) {
					VectorElemHD currElem = valItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem.toStringPart() + '\n');
						counter++;
					} catch (IOException e) {
						log.debug("BR WindowID != -1: Error writing remaining elements in que");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}

				Iterator<VectorElemHD> inMemoryItr = inMemoryList.iterator();
				while (inMemoryItr.hasNext()) {
					VectorElemHD currElem = inMemoryItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem.toStringPart() + '\n');
						counter++;
					} catch (IOException e) {
						log.debug("BR WindowID != -1: Error writing inMemoryList elements");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}
				for (int i = 0; i < out.length; i++)
					try {
						out[i].close();
					} catch (IOException e) {
						log.debug("BR WindowID != -1: Error closing outfiles");
						e.printStackTrace();
					}
			}// end it's a window partition
		}// end out of memory operations
		log.debug("Intermediate elements commited to HDFS: " + counter);
		log.debug("===================================Base Reduce End===================================");
	}// end reduce

	private void baseQuickJoin(List<VectorElemHD> objs, double eps,
			Context context, long code) {
		log.debug("Begin: baseQuickJoin size: " + objs.size());
		// log.debug("objs.size() = " + objs.size() + ": baseQuickJoin");
		if (objs.size() < constSmallNum) {
			// log.debug("Call Nested Loop: baseQuickJoin");
			baseNestedLoop(objs, eps, context, code);
			return;
		} else {
			int p1Index;
			int p2Index;

			p1Index = (int) (Math.random() * (objs.size()));
			do {
				p2Index = (int) (Math.random() * (objs.size()));
			} while (p1Index == p2Index);

			log.debug("P1Index " + objs.get(p1Index).toString() +": baseQuickJoin");
			log.debug("P2Index " + objs.get(p2Index).toString() + ": baseQuickJoin");

			// log.debug("Call basePartition: baseQuickJoin");
			basePartition(objs, eps, context, code, objs.get(p1Index), objs
					.get(p2Index));
		}
	}// end BaseQuickJoin

	private void basePartition(List<VectorElemHD> objs, double eps,Context context, long code, VectorElemHD p1, VectorElemHD p2) {
		
		log.debug("Begin: basePartition size: " + objs.size());
		int r = 0;
		int startIndx = 0;
		int endIndx = objs.size() - 1;

		ArrayList<VectorElemHD> winP1 = new ArrayList<VectorElemHD>();
		ArrayList<VectorElemHD> winP2 = new ArrayList<VectorElemHD>();
		
	
		double startDist = (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension) - objs.get(startIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		
		double endDist = (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) - objs	.get(endIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		log.debug("***********\n" +
				"objs.get(startIndx):"+objs.get(startIndx).toStringAll()+"\n" +
				"objs.get(startIndx).size:"+objs.get(startIndx).getElemList().size()+"\n" +
				"start--p1:"+objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)+"\n" +
				"start--p2:"+objs.get(startIndx).getDistanceByEmd(p2, bins,dimension)+"\n" +
				"end--p1:"+objs.get(endIndx).getDistanceByEmd(p1, bins,dimension)+"\n" +
				"end--p2:"+objs.get(endIndx).getDistanceByEmd(p2, bins,dimension)+"\n" +
				"statDist:"+startDist+"\n" +
				"endDist:"+endDist+"\n" +
				"startIndx:"+startIndx+"\n" +
				"endIndx:"+endIndx+"\n" +
				"");		
		log.debug("***********");
		
		while (startIndx < endIndx) {

			while (endDist > r && startIndx < endIndx) {// closest to p2
				if (endDist <= (r + (eps + errorAdjsmt)))
					winP1.add(objs.get(endIndx));
				endIndx--;				
				endDist = (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) - objs
						.get(endIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			while (startDist <= r && startIndx < endIndx) {// closest to p1
				if (startDist >= (r - (eps + errorAdjsmt))
						&& startIndx < endIndx)
					winP2.add(objs.get(startIndx));
				startIndx++;				
				startDist = (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension) - objs
						.get(startIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			if (startIndx < endIndx) {
				if (endDist >= (r - (eps + errorAdjsmt)))
					winP2.add(objs.get(endIndx));
				if (startDist <= (r + (eps + errorAdjsmt)))
					winP1.add(objs.get(startIndx));

				// exchange positions//
				VectorElemHD temp = objs.get(startIndx);
				objs.set(startIndx, objs.get(endIndx));
				objs.set(endIndx, temp);
				// ////////////////////

				startIndx++;				
				startDist = (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension) - objs	.get(startIndx).getDistanceByEmd(p2, bins,dimension)) / 2;

				endIndx--;				
				endDist = (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) - objs.get(endIndx).getDistanceByEmd(p2, bins,dimension)) / 2;

			}

		}// end outer while loop
			
		
		if (startIndx == endIndx) {
			if (endDist > r && endDist <= (r + (eps + errorAdjsmt)))
				winP1.add(objs.get(endIndx));

			if (startDist <= r && startDist >= (r - (eps + errorAdjsmt)))
				winP2.add(objs.get(startIndx));

			if (endDist > r)
				endIndx--;
		}
		
		baseQuickJoin(objs.subList(0, endIndx + 1), eps, context, code);
		baseQuickJoin(objs.subList(endIndx + 1, objs.size()), eps, context,code);
		
		// log.debug("trim: basePartition");
		winP1.trimToSize();
		winP2.trimToSize();
		
		// log.debug("Call baseQuickJoinWin: basePartition");
		baseQuickJoinWin(winP1.subList(0, winP1.size()), winP2.subList(0,
				winP2.size()), eps, context, code);
		
		context.progress();
	}// end basePartition

	private void baseQuickJoinWin(List<VectorElemHD> objs1,
			List<VectorElemHD> objs2, double eps, Context context, long code) {
		log.debug("Begin: baseQuickJoinWin size1: " + objs1.size()
				+ " size2: " + objs2.size());
		int totalSize = (objs1.size() + objs2.size());
		if (totalSize < constSmallNum) {
			// log.debug("Call Nested Loop: baseQuickJoinWin");
			baseNestedLoop(objs1, objs2, eps, context, code);
			return;
		} else {
			int p1Index;
			int p2Index;

			p1Index = (int) (Math.random() * (totalSize));
			do {
				p2Index = (int) (Math.random() * (totalSize));
			} while (p1Index == p2Index);

			if (objs1.size() != 0 && objs2.size() != 0) {
				// log.debug("Call basePartitionWin 01: baseQuickJoinWin");
				basePartitionWin(objs1, objs2, eps, context, code,
						(p1Index < objs1.size() ? (objs1.get(p1Index))
								: (objs2.get(p1Index - objs1.size()))),
						(p2Index < objs1.size() ? (objs1.get(p2Index))
								: (objs2.get(p2Index - objs1.size()))));
			} else if (objs1.size() == 0) {// objs1 is empty, use objs2
				// log.debug("Call basePartitionWin 02: baseQuickJoinWin");
				// basePartitionWin(objs1, objs2, eps, context,
				// objs2.get(p1Index), objs2.get(p2Index));
				return;
			} else {// objs2 is empty, use objs1
				// log.debug("Call basePartitionWin 03: baseQuickJoinWin");
				// basePartitionWin(objs1, objs2, eps, context,
				// objs1.get(p1Index), objs1.get(p2Index));
				return;
			}
		}

	}// end baseQuickJoinWin

	private void basePartitionWin(List<VectorElemHD> objs1,
			List<VectorElemHD> objs2, double eps, Context context,
			long code, VectorElemHD p1, VectorElemHD p2) {
		log.debug("Begin: basePartitionWin size1: " + objs1.size()
				+ " size2: " + objs2.size());
		int r = 0;

		// ///////////////////////////////////////////////////////////////////////////////////////////
		int o1StartIndx = 0;
		int o1EndIndx = objs1.size() - 1;

		ArrayList<VectorElemHD> o1WinP1 = new ArrayList<VectorElemHD>();
		ArrayList<VectorElemHD> o1WinP2 = new ArrayList<VectorElemHD>();

		// double o1StartDist = (
		// (objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension) *
		// objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension)) -
		// (objs1.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension) *
		// objs1.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
		// p1.getDistanceByEmd(p2, bins,dimension));
		double o1StartDist = (objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension) - objs1
				.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		// double o1EndDist = ( (objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension)
		// * objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension)) -
		// (objs1.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension) *
		// objs1.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
		// p1.getDistanceByEmd(p2, bins,dimension));
		double o1EndDist = (objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension) - objs1
				.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		while (o1StartIndx < o1EndIndx) {

			while (o1EndDist > r && o1StartIndx < o1EndIndx) {// closest to
				// p2
				if (o1EndDist <= (r + (eps + errorAdjsmt)))
					o1WinP1.add(objs1.get(o1EndIndx));
				o1EndIndx--;
				// o1EndDist = (
				// (objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs1.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs1.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o1EndDist = (objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension) - objs1
						.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			while (o1StartDist <= r && o1StartIndx < o1EndIndx) {// closest
				// to p1
				if (o1StartDist >= (r - (eps + errorAdjsmt)))
					o1WinP2.add(objs1.get(o1StartIndx));
				o1StartIndx++;
				// o1StartDist = (
				// (objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs1.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs1.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o1StartDist = (objs1.get(o1StartIndx)
						.getDistanceByEmd(p1, bins,dimension) - objs1.get(o1StartIndx)
						.getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			if (o1StartIndx < o1EndIndx) {
				if (o1EndDist >= (r - (eps + errorAdjsmt)))
					o1WinP2.add(objs1.get(o1EndIndx));
				if (o1StartDist <= (r + (eps + errorAdjsmt)))
					o1WinP1.add(objs1.get(o1StartIndx));

				// exchange positions//
				VectorElemHD temp = objs1.get(o1StartIndx);
				objs1.set(o1StartIndx, objs1.get(o1EndIndx));
				objs1.set(o1EndIndx, temp);
				// ////////////////////

				o1StartIndx++;
				// o1StartDist = (
				// (objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs1.get(o1StartIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs1.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs1.get(o1StartIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o1StartDist = (objs1.get(o1StartIndx)
						.getDistanceByEmd(p1, bins,dimension) - objs1.get(o1StartIndx)
						.getDistanceByEmd(p2, bins,dimension)) / 2;

				o1EndIndx--;
				// o1EndDist = (
				// (objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs1.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs1.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o1EndDist = (objs1.get(o1EndIndx).getDistanceByEmd(p1, bins,dimension) - objs1
						.get(o1EndIndx).getDistanceByEmd(p2, bins,dimension)) / 2;

			}

		}// end outer while loop

		if (o1StartIndx == o1EndIndx) {
			if (o1EndDist > r && o1EndDist <= (r + (eps + errorAdjsmt)))
				o1WinP1.add(objs1.get(o1EndIndx));

			if (o1StartDist <= r
					&& o1StartDist >= (r - (eps + errorAdjsmt)))
				o1WinP2.add(objs1.get(o1StartIndx));

			if (o1EndDist > r)
				o1EndIndx--;
		}
		// /////////////////////////////////////////////////////

		// /////////////////////////////////////////////////////
		int o2StartIndx = 0;
		int o2EndIndx = objs2.size() - 1;

		ArrayList<VectorElemHD> o2WinP1 = new ArrayList<VectorElemHD>();
		ArrayList<VectorElemHD> o2WinP2 = new ArrayList<VectorElemHD>();

		// double o2StartDist = (
		// (objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension) *
		// objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension)) -
		// (objs2.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension) *
		// objs2.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
		// p1.getDistanceByEmd(p2, bins,dimension));
		double o2StartDist = (objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension) - objs2
				.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		// double o2EndDist = ( (objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension)
		// * objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension)) -
		// (objs2.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension) *
		// objs2.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
		// p1.getDistanceByEmd(p2, bins,dimension));
		double o2EndDist = (objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension) - objs2
				.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		while (o2StartIndx < o2EndIndx) {

			while (o2EndDist > r && o2StartIndx < o2EndIndx) {// closest to
				// p2
				if (o2EndDist <= (r + (eps + errorAdjsmt)))
					o2WinP1.add(objs2.get(o2EndIndx));
				o2EndIndx--;
				// o2EndDist = (
				// (objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs2.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs2.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o2EndDist = (objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension) - objs2
						.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			while (o2StartDist <= r && o2StartIndx < o2EndIndx) {// closest
				// to p1
				if (o2StartDist >= (r - (eps + errorAdjsmt)))
					o2WinP2.add(objs2.get(o2StartIndx));
				o2StartIndx++;
				// o2StartDist = (
				// (objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs2.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs2.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o2StartDist = (objs2.get(o2StartIndx)
						.getDistanceByEmd(p1, bins,dimension) - objs2.get(o2StartIndx)
						.getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			if (o2StartIndx < o2EndIndx) {
				if (o2EndDist >= (r - (eps + errorAdjsmt)))
					o2WinP2.add(objs2.get(o2EndIndx));
				if (o2StartDist <= (r + (eps + errorAdjsmt)))
					o2WinP1.add(objs2.get(o2StartIndx));

				// exchange positions//
				VectorElemHD temp = objs2.get(o2StartIndx);
				objs2.set(o2StartIndx, objs2.get(o2EndIndx));
				objs2.set(o2EndIndx, temp);
				// ////////////////////

				o2StartIndx++;
				// o2StartDist = (
				// (objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs2.get(o2StartIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs2.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs2.get(o2StartIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o2StartDist = (objs2.get(o2StartIndx)
						.getDistanceByEmd(p1, bins,dimension) - objs2.get(o2StartIndx)
						.getDistanceByEmd(p2, bins,dimension)) / 2;

				o2EndIndx--;
				// o2EndDist = (
				// (objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs2.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs2.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				o2EndDist = (objs2.get(o2EndIndx).getDistanceByEmd(p1, bins,dimension) - objs2
						.get(o2EndIndx).getDistanceByEmd(p2, bins,dimension)) / 2;

			}

		}// end outer while loop

		if (o2StartIndx == o2EndIndx) {
			if (o2EndDist > r && o2EndDist <= (r + (eps + errorAdjsmt)))
				o2WinP1.add(objs2.get(o2EndIndx));

			if (o2StartDist <= r
					&& o2StartDist >= (r - (eps + errorAdjsmt)))
				o2WinP2.add(objs2.get(o2StartIndx));

			if (o2EndDist > r)
				o2EndIndx--;
		}

		// log.debug("trim: basePartitionWin");
		o1WinP1.trimToSize();
		o1WinP2.trimToSize();
		o2WinP1.trimToSize();
		o2WinP2.trimToSize();

		// log.debug("Call baseQuickJoinWin: basePartitionWin");
		baseQuickJoinWin(o1WinP1.subList(0, o1WinP1.size()), o2WinP2
				.subList(0, o2WinP2.size()), eps, context, code);
		baseQuickJoinWin(o1WinP2.subList(0, o1WinP2.size()), o2WinP1
				.subList(0, o2WinP1.size()), eps, context, code);
		baseQuickJoinWin(objs1.subList(0, o1EndIndx + 1), objs2.subList(0,
				o2EndIndx + 1), eps, context, code);
		baseQuickJoinWin(objs1.subList(o1EndIndx + 1, objs1.size()), objs2
				.subList(o2EndIndx + 1, objs2.size()), eps, context, code);

	}

	private void baseNestedLoop(List<VectorElemHD> objs1,
			List<VectorElemHD> objs2, double eps, Context context, long code) {
		// log.debug("BR performInMemoryJoin - Window");
		// Iterator<StringElem> outer = inMemoryList.iterator(); //nested
		// loop join for in memory join (n^2): NEEDS IMPROVEMENT
		// while (outer.hasNext())
		for (int i = 0; i < objs1.size(); i++) {
			VectorElemHD outerElem = objs1.get(i);
			// log.debug("BR SimJoin (outer): " + outerElem.toString());
			for (int j = 0; j < objs2.size(); j++) {
				// StringElem innerElem = inner.next();
				VectorElemHD innerElem = objs2.get(j);
				// log.debug("BR SimJoin (inner): " + innerElem.toString());
//				if ((innerElem.getKey() % 2) != (outerElem.getKey() % 2)) {
					if (code == -1) {

						if (innerElem.getKey() != outerElem.getKey()) {
							boolean candidate = true;
							double [] histA = list2array(innerElem.getElemList());
							double [] histB = list2array(outerElem.getElemList());
							for (int k = 0; k < numVector; k++) {
								eachProjection = FormatUtil.getSubArray(projectedBins, k*numBins, (k+1)*numBins - 1, eachProjection);
								if (HistUtil.getProjectEmd(histA, histB, eachProjection) > eps) {
									candidate = false;
								}
							}
							
							if (!candidate) continue;
							// dual
							if (dualCounter < numDual) {
								duals[dualCounter] = new DualBound(histA, histB, bins, dimension);
								dualCounter++;
							}
							for (int k = 0; k < dualCounter; k++) {
								if (duals[k].getDualEmd(histA, histB) > eps) {
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// reduced
							for (int k = 0; k < numReduced; k++) {
								if (reductions[k].getReducedEmd(histA, histB) > eps) {
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// independent minimization
							if (DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null) > eps) {
								candidate = false;
							}							
							
							if (!candidate) continue;
							if (candidate) {
								double diffDist = outerElem
										.getDistanceByEmd(innerElem, bins,dimension);
								// log.debug("BR Checking: Elem: " +
								// outerElem.toString() + " Against: " +
								// innerElem.toString() + " With Distance: " +
								// diffDist);
								if (diffDist <= eps) // checks if the difference
								// in distance is less
								// than eps
								{ // may be modified for more detailed
									// output?????
									Text t1 = new Text(outerElem.toString()); // set
									// up
									// output
									// for
									// joined
									// pair
									Text t2 = new Text(innerElem.toString());
									// log.debug("BR we found a match: " +
									// t1.toString() + " : " + t2.toString());
									// log.debug("BR we found a match: " +
									// t2.toString() + " : " + t1.toString());
									try {
										context.write(t1, t2);
	//									context.write(t2, t1);
									} catch (IOException e) {
										log
												.error("BR Error writing to context.");
									} catch (InterruptedException e) {
										log
												.error("BR Error writing to context.");
									}
	
								}
							}
						}
					}// end it's not a window
					else {
						if (outerElem.getPartitionID() != innerElem.getPartitionID()) {
							boolean candidate = true;
							double [] histA = list2array(innerElem.getElemList());
							double [] histB = list2array(outerElem.getElemList());
							for (int k = 0; k < numVector; k++) {
								eachProjection = FormatUtil.getSubArray(projectedBins, k*numBins, (k+1)*numBins - 1, eachProjection);
								if (HistUtil.getProjectEmd(histA, histB, eachProjection) > eps) {
									candidate = false;
								}
							}

							if (!candidate) continue;
							// dual
							if (dualCounter < numDual) {
								duals[dualCounter] = new DualBound(histA, histB, bins, dimension);
								dualCounter++;
							}
							for (int k = 0; k < dualCounter; k++) {
								if (duals[k].getDualEmd(histA, histB) > eps) {
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// reduced
							for (int k = 0; k < numReduced; k++) {
								if (reductions[k].getReducedEmd(histA, histB) > eps) {
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// independent minimization
							if (DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null) > eps) {
								candidate = false;
							}							
							
							if (candidate) {
							double diffDist = outerElem
									.getDistanceByEmd(innerElem, bins,dimension);
								// log.debug("BR Checking: Elem: " +
								// outerElem.toString() + " Against: " +
								// innerElem.toString() + " With Distance: " +
								// diffDist);
								if (diffDist <= eps) // checks if the difference
								// in distance is less
								// than eps
								{ // may be modified for more detailed
									// output?????
									Text t1 = new Text(outerElem.toString()); // set
									// up
									// output
									// for
									// joined
									// pair
									Text t2 = new Text(innerElem.toString());
									// log.debug("BR we found a match: " +
									// t1.toString() + " : " + t2.toString());
									// log.debug("BR we found a match: " +
									// t2.toString() + " : " + t1.toString());
									try {
										context.write(t1, t2); // write to the
										// output both
										// pairs
	//									context.write(t2, t1);
									} catch (IOException e) {
										System.out
												.println("Error writing to context.");
									} catch (InterruptedException e) {
										System.out
												.println("Error writing to context.");
									}
								}
							}
						}
					}// end it is a window
//				}

			}// end inner loop
		}// end outer loop

	}

	private void baseNestedLoop(List<VectorElemHD> objs, double eps,
			Context context, long code) {
		for (int i = 0; i < objs.size(); i++) {
			VectorElemHD outerElem = objs.get(i);
			// log.debug("BR SimJoin (outer): " + outerElem.toString());
			for (int j = i+1; j < objs.size(); j++) {
				VectorElemHD innerElem = objs.get(j);
				// log.debug("BR SimJoin (inner): " + innerElem.toString());
//				if ((innerElem.getKey() % 2) != (outerElem.getKey() % 2)) {
					if (code == -1) {

						if (innerElem.getKey() != outerElem.getKey()) {
							boolean candidate = true;
							double [] histA = list2array(innerElem.getElemList());
							double [] histB = list2array(outerElem.getElemList());
							//System.out.println("Join " + FormatUtil.toString(histA) + " and " + FormatUtil.toString(histB));
							for (int k = 0; k < numVector; k++) {
								
								
								
								eachProjection = FormatUtil.getSubArray(projectedBins, k*numBins, (k+1)*numBins - 1, eachProjection);
								if (HistUtil.getProjectEmd(histA, histB, eachProjection) > eps) {
									candidate = false;
								//	System.out.println("eliminated by projection");
									break;
								}
							}
							
						
							
							if (!candidate) continue;
							// dual
							if (dualCounter < numDual) {
								duals[dualCounter] = new DualBound(histA, histB, bins, dimension);
								dualCounter++;
							}
							for (int k = 0; k < dualCounter; k++) {
								if (duals[k].getDualEmd(histA, histB) > eps) {
								//	System.out.println("eliminated by dual");
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// reduced
							for (int k = 0; k < numReduced; k++) {
								if (reductions[k].getReducedEmd(histA, histB) > eps) {
								//	System.out.println("eliminated by reduced");
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// independent minimization
							if (DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null) > eps) {
								//System.out.println("eliminated by ind min");
								candidate = false;
							}							
							
							if (candidate) {
								double diffDist = outerElem
									.getDistanceByEmd(innerElem, bins,dimension);
								// log.debug("BR Checking: Elem: " +
								// outerElem.toString() + " Against: " +
								// innerElem.toString() + " With Distance: " +
								// diffDist);
								if (diffDist <= eps) // checks if the difference
									
								// in distance is less
								// than eps
								{ // may be modified for more detailed
									// output?????
									//System.out.println("computed " + diffDist);
									
									Text t1 = new Text(outerElem.toString()); // set up output for joined pair
									Text t2 = new Text(innerElem.toString());
									// log.debug("BR we found a match: " +
									// t1.toString() + " : " + t2.toString());
									try {
										context.write(t1, t2);
									} catch (IOException e) {
										log
												.error("BR Error writing to context.");
									} catch (InterruptedException e) {
										log
												.error("BR Error writing to context.");
									}
	
								}
							}
						}
					}// end it's not a window
					else {
						if (outerElem.getPartitionID() != innerElem.getPartitionID()) {
							boolean candidate = true;
							double [] histA = list2array(innerElem.getElemList());
							double [] histB = list2array(outerElem.getElemList());
							for (int k = 0; k < numVector; k++) {
								eachProjection = FormatUtil.getSubArray(projectedBins, k*numBins, (k+1)*numBins - 1, eachProjection);
								if (HistUtil.getProjectEmd(histA, histB, eachProjection) > eps) {
									candidate = false;
									//out.println("eliminated by projection");
									break;
								}
							}
							
							if (!candidate) continue;
							// dual
							if (dualCounter < numDual) {
								duals[dualCounter] = new DualBound(histA, histB, bins, dimension);
								dualCounter++;
							}
							for (int k = 0; k < dualCounter; k++) {
								if (duals[k].getDualEmd(histA, histB) > eps) {
									//System.out.println("eliminated by dual");
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// reduced
							for (int k = 0; k < numReduced; k++) {
								if (reductions[k].getReducedEmd(histA, histB) > eps) {
									//System.out.println("eliminated by reduced");
									candidate = false;
									break;
								}
							}
							
							if (!candidate) continue;
							// independent minimization
							if (DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null) > eps) {
								//System.out.println("eliminated by ind min");
								candidate = false;
							}
							
							if (candidate) {
								double diffDist = outerElem.getDistanceByEmd(innerElem, bins,dimension);
								// log.debug("BR Checking: Elem: " +
								// outerElem.toString() + " Against: " +
								// innerElem.toString() + " With Distance: " +
								// diffDist);
								if (diffDist <= eps) // checks if the difference
								// in distance is less
								// than eps
								{ // may be modified for more detailed output?????
									
									//System.out.println("computed " + diffDist);
									
									Text t1 = new Text(outerElem.toString()); // set
									// up
									// output
									// for
									// joined
									// pair
									Text t2 = new Text(innerElem.toString());
									// log.debug("BR we found a match: " +
									// t1.toString() + " : " + t2.toString());
									try {
										context.write(t1, t2); // write to the
										// output both
										// pairs
									} catch (IOException e) {
										System.out
												.println("Error writing to context.");
									} catch (InterruptedException e) {
										System.out
												.println("Error writing to context.");
									}
								}
							}
						}
					}// end it is a window
//				}

			}// end inner loop
		}// end outter loop

	}// end baseNestedLoop
	public VectorElemHD copyVectorElem(VectorElemHD o){
		VectorElemHD ve = new VectorElemHD();
		ve.setPartitionID(o.getPartitionID());
		ve.setPrevPartition(o.getPrevPartition());
		ve.setWindowID(o.getWindowID());
		ve.setKey(o.getKey());
		List<Double> elemList = new ArrayList<Double>();
		List<Double> el=o.getElemList();
		for(Double d:el)
			elemList.add(d);
		ve.setElemList(elemList);
		return ve;
	}
	
	public double [] list2array(List<Double> list){
		double[] hist = new double[list.size()];
		int i=0;
		for(Double d:list)
			hist[i++]=d;
		return hist;
	}	
}
