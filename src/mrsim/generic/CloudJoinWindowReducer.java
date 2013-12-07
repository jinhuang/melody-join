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

public class CloudJoinWindowReducer  extends Reducer<CloudJoinKey, VectorElemHD, Text, Text>{

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
		log
				.debug("===================================Window Reduce Begin===================================");
		String s;
		s = context.getConfiguration().get("eps", "");
		if ("".equals(s)) {
			System.out
					.println("Error: Unable to get eps from configuration");
		}
		double eps = Double.parseDouble(s);
		s = context.getConfiguration().get("memory", "");
		if ("".equals(s)) {
			System.out
					.println("Error: Unable to get memory from configuration");
		}
		long memory = Long.parseLong(s);
		s = context.getConfiguration().get("itr", "");
		if ("".equals(s)) {
			System.out
					.println("Error: Unable to get itr from configuration");
		}
		long itr = Long.parseLong(s);
		String outDir = context.getConfiguration().get("outDir", "");
		if ("".equals(outDir)) {
			System.out
					.println("Error: Unable to get outDir from configuration");
		}
		long sizeOfElem = -1;
		boolean performInMemoryJoin = true;
		ArrayList<VectorElemHD> inMemoryList = new ArrayList<VectorElemHD>();

		log.debug("WR start inmemory collection");
		// log.debug("WR CJK: " + key.toString());
		Iterator<VectorElemHD> valItr = values.iterator();
		int counter = 0;
		while (valItr.hasNext()) {
			VectorElemHD currElem = copyVectorElem(valItr.next());

			// log.debug("BR collecting: " + currElem.toString());

			if (sizeOfElem == -1)// could we set this outside the loop?
				sizeOfElem = currElem.getSizeInBytes();

			inMemoryList.add(counter, currElem);
			counter++;
			// log.debug("R collecting(added?): " +
			// inMemoryList.get(inMemoryList.size() - 1).toString());

			// if we have reached the limit of memory and there is more to
			// do, break and don't perform inMemoryJoin
			if ((memory <= ((inMemoryList.size()) * sizeOfElem))
					&& valItr.hasNext()) {
				log.debug("WR we are not performing in memory join");
				performInMemoryJoin = false;
				break;
			}
		}

		log.debug("WR end inmemory collection");
		// log.debug("WR size of inMemoryList: " + inMemoryList.size());

		if (performInMemoryJoin) {
			// for all elements in inMemoryList,
			// check if they can be joined to all others
			// if they can, write output to final output
			log.debug("WR performInMemoryJoin - Base");
			inMemoryList.trimToSize();
			windowQuickJoin(inMemoryList.subList(0, inMemoryList.size()),
					eps, context, key.getWindowID());

		} else// perform out of memory operations
		{
			FileSystem fs; // create variable for file system access
			Path outfile;
			Path itrOutDir;
			FSDataOutputStream[] out = new FSDataOutputStream[numFilesToGen];
			counter = 0;
			log.debug("WR performOutOfMemoryOperation");
			if (key.getWindowID() == -1) // it's a normal partition
			{
				try {
					fs = FileSystem.get(context.getConfiguration()); // try
					// to
					// set
					// up
					// a
					// output
					// stream
					// to
					// the
					// HDFS
					itrOutDir = new Path(outDir + "intermediate_output_"
							+ itr + "_"
							+ context.getConfiguration().get("V") + "_"
							+ context.getConfiguration().get("W") + "_"
							+ key.getPartitionID());
					fs.mkdirs(itrOutDir);

					for (int i = 0; i < numFilesToGen; i++) {
						outfile = new Path(itrOutDir.toString() + "/" + itr
								+ "_" + i + ".txt");
						out[i] = fs.create(outfile);
					}
					log.debug("Output Dir: " + itrOutDir.toString());
				} catch (IOException e) {
					log
							.debug("WR WindowID = -1: Error creating directory/file");
					log.debug(e.getMessage()); // error: unable to create
					// stream
					e.printStackTrace(); // error: unable to create stream
					return;
				}

				while (valItr.hasNext()) {
					VectorElemHD currElem = valItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem
								.toStringPrev() + '\n');
						counter++;
					} catch (IOException e) {
						log
								.debug("WR WindowID = -1: Error writing remaining elements in que");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}

				Iterator<VectorElemHD> inMemoryItr = inMemoryList
						.iterator();
				while (inMemoryItr.hasNext()) {
					VectorElemHD currElem = inMemoryItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem
								.toStringPrev() + '\n');
						counter++;
					} catch (IOException e) {
						log
								.debug("WR WindowID = -1: Error writing inMemoryList elements");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}
				for (int i = 0; i < out.length; i++)
					try {
						out[i].close();
					} catch (IOException e) {
						log
								.debug("BR WindowID != -1: Error closing outfiles");
						e.printStackTrace();
					}
			} else // it's a window partition
			{
				try {
					fs = FileSystem.get(context.getConfiguration()); // try
					// to
					// set
					// up
					// a
					// output
					// stream
					// to
					// the
					// HDFS
					itrOutDir = new Path(outDir + "intermediate_output_"
							+ itr + "_" + key.getPartitionID() + "_"
							+ key.getWindowID() + "_"
							+ key.getPrevItrPartion());
					fs.mkdirs(itrOutDir);

					for (int i = 0; i < numFilesToGen; i++) {
						outfile = new Path(itrOutDir.toString() + "/" + itr
								+ "_" + i + ".txt");
						out[i] = fs.create(outfile);
					}
					log.debug("Output Dir: " + itrOutDir.toString());

				} catch (IOException e) {
					log
							.debug("WR WindowID != -1: Error creating directory/file");
					log.debug(e.getMessage()); // error: unable to create
					// stream
					e.printStackTrace(); // error: unable to create stream
					return;
				}

				while (valItr.hasNext()) {
					VectorElemHD currElem = valItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem
								.toStringPart() + '\n');
						counter++;
					} catch (IOException e) {
						log
								.debug("WR WindowID != -1: Error writing remaining elements in que");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}

				Iterator<VectorElemHD> inMemoryItr = inMemoryList
						.iterator();
				while (inMemoryItr.hasNext()) {
					VectorElemHD currElem = inMemoryItr.next();
					try {
						out[counter % numFilesToGen].writeBytes(currElem
								.toStringPart() + '\n');
						counter++;
					} catch (IOException e) {
						log
								.debug("WR WindowID != -1: Error writing inMemoryList elements");
						log.debug(e.getMessage());
						e.printStackTrace();
					}
				}
				for (int i = 0; i < out.length; i++)
					try {
						out[i].close();
					} catch (IOException e) {
						log
								.debug("BR WindowID != -1: Error closing outfiles");
						e.printStackTrace();
					}
			}
			log.debug("Intermediate elements commited to HDFS: " + counter);
		}

		log
				.debug("===================================Window Reduce End===================================");
	}// end reduce

	private void windowQuickJoin(List<VectorElemHD> objs, double eps,
			Context context, long code) {
		log.debug("Begin: windowQuickJoin size1: " + objs.size());
		// log.debug("objs.size() = " + objs.size() + ": windowQuickJoin");
		if (objs.size() < constSmallNum) {
			// log.debug("Call Nested Loop: windowQuickJoin");
			windowNestedLoop(objs, eps, context, code);
			return;
		} else {
			int p1Index;
			int p2Index;

			p1Index = (int) (Math.random() * (objs.size()));
			do {
				p2Index = (int) (Math.random() * (objs.size()));
			} while (p1Index == p2Index);

			// log.debug("P1Index " + objs.get(p1Index).toString() +
			// ": windowQuickJoin");
			// log.debug("P2Index " + objs.get(p2Index).toString() +
			// ": windowQuickJoin");

			// log.debug("Call basePartition: windowQuickJoin");
			windowPartition(objs, eps, context, code, objs.get(p1Index),
					objs.get(p2Index));
		}
	}// end BaseQuickJoin

	private void windowPartition(List<VectorElemHD> objs, double eps,
			Context context, long code, VectorElemHD p1, VectorElemHD p2) {
		log.debug("Begin: windowPartition size1: " + objs.size());
		int r = 0;
		int startIndx = 0;
		int endIndx = objs.size() - 1;

		ArrayList<VectorElemHD> winP1 = new ArrayList<VectorElemHD>();
		ArrayList<VectorElemHD> winP2 = new ArrayList<VectorElemHD>();

		// double startDist = ( (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)
		// * objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)) -
		// (objs.get(startIndx).getDistanceByEmd(p2, bins,dimension) *
		// objs.get(startIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
		// p1.getDistanceByEmd(p2, bins,dimension));
		double startDist = (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension) - objs
				.get(startIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		// double endDist = ( (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) *
		// objs.get(endIndx).getDistanceByEmd(p1, bins,dimension)) -
		// (objs.get(endIndx).getDistanceByEmd(p2, bins,dimension) *
		// objs.get(endIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
		// p1.getDistanceByEmd(p2, bins,dimension));
		double endDist = (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) - objs
				.get(endIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
		while (startIndx < endIndx) {

			while (endDist > r && startIndx < endIndx) {// closest to p2
				if (endDist <= (r + (eps + errorAdjsmt)))
					winP1.add(objs.get(endIndx));
				endIndx--;
				// endDist = ( (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs.get(endIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs.get(endIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs.get(endIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				endDist = (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) - objs
						.get(endIndx).getDistanceByEmd(p2, bins,dimension)) / 2;
			}

			while (startDist <= r && startIndx < endIndx) {// closest to p1
				if (startDist >= (r - (eps + errorAdjsmt))
						&& startIndx < endIndx)
					winP2.add(objs.get(startIndx));
				startIndx++;
				// startDist = ( (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)
				// * objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs.get(startIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs.get(startIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
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
				// startDist = ( (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)
				// * objs.get(startIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs.get(startIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs.get(startIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				startDist = (objs.get(startIndx).getDistanceByEmd(p1, bins,dimension) - objs
						.get(startIndx).getDistanceByEmd(p2, bins,dimension)) / 2;

				endIndx--;
				// endDist = ( (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) *
				// objs.get(endIndx).getDistanceByEmd(p1, bins,dimension)) -
				// (objs.get(endIndx).getDistanceByEmd(p2, bins,dimension) *
				// objs.get(endIndx).getDistanceByEmd(p2, bins,dimension)) ) / (2 *
				// p1.getDistanceByEmd(p2, bins,dimension));
				endDist = (objs.get(endIndx).getDistanceByEmd(p1, bins,dimension) - objs
						.get(endIndx).getDistanceByEmd(p2, bins,dimension)) / 2;

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

		// log.debug("objs.size() = " + objs.size() + ": windowPartition");
		// log.debug("endIndx = " + endIndx + ": windowPartition");

		// log.debug("Call windowQuickJoin: windowPartition");
		windowQuickJoin(objs.subList(0, endIndx + 1), eps, context, code);
		windowQuickJoin(objs.subList(endIndx + 1, objs.size()), eps,
				context, code);
		// log.debug("trim: windowPartition");
		winP1.trimToSize();
		winP2.trimToSize();
		// log.debug("Call windoweQuickJoinWin: windowPartition");
		windowQuickJoinWin(winP1.subList(0, winP1.size()), winP2.subList(0,
				winP2.size()), eps, context, code);

	}// end windowPartition

	private void windowQuickJoinWin(List<VectorElemHD> objs1,
			List<VectorElemHD> objs2, double eps, Context context, long code) {
		log.debug("Begin: windowQuickJoinWin size1: " + objs1.size()
				+ " size2: " + objs2.size());
		int totalSize = (objs1.size() + objs2.size());
		if (totalSize < constSmallNum) {
			// log.debug("Call Nested Loop: windowQuickJoinWin");
			windowNestedLoop(objs1, objs2, eps, context, code);
			return;
		} else {
			int p1Index;
			int p2Index;

			p1Index = (int) (Math.random() * (totalSize));
			do {
				p2Index = (int) (Math.random() * (totalSize));
			} while (p1Index == p2Index);

			if (objs1.size() != 0 && objs2.size() != 0) {
				// log.debug("Call windowPartitionWin 01: windowQuickJoinWin");
				windowPartitionWin(objs1, objs2, eps, context, code,
						(p1Index < objs1.size() ? (objs1.get(p1Index))
								: (objs2.get(p1Index - objs1.size()))),
						(p2Index < objs1.size() ? (objs1.get(p2Index))
								: (objs2.get(p2Index - objs1.size()))));
			} else if (objs1.size() == 0) {// objs1 is empty, use objs2
				// log.debug("Call windowPartitionWin 02: windowQuickJoinWin");
				// basePartitionWin(objs1, objs2, eps, context,
				// objs2.get(p1Index), objs2.get(p2Index));
				return;
			} else {// objs2 is empty, use objs1
				// log.debug("Call windowPartitionWin 03: windowQuickJoinWin");
				// basePartitionWin(objs1, objs2, eps, context,
				// objs1.get(p1Index), objs1.get(p2Index));
				return;
			}
		}

	}

	private void windowPartitionWin(List<VectorElemHD> objs1,
			List<VectorElemHD> objs2, double eps, Context context,
			long code, VectorElemHD p1, VectorElemHD p2) {
		log.debug("Begin: windowPartitionWin size1: " + objs1.size()
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

		// log.debug("trim: windowPartitionWin");
		o1WinP1.trimToSize();
		o1WinP2.trimToSize();
		o2WinP1.trimToSize();
		o2WinP2.trimToSize();

		// log.debug("Call windowQuickJoinWin:windowPartitionWin");
		windowQuickJoinWin(o1WinP1.subList(0, o1WinP1.size()), o2WinP2
				.subList(0, o2WinP2.size()), eps, context, code);
		windowQuickJoinWin(o1WinP2.subList(0, o1WinP2.size()), o2WinP1
				.subList(0, o2WinP1.size()), eps, context, code);
		windowQuickJoinWin(objs1.subList(0, o1EndIndx + 1), objs2.subList(
				0, o2EndIndx + 1), eps, context, code);
		windowQuickJoinWin(objs1.subList(o1EndIndx + 1, objs1.size()),
				objs2.subList(o2EndIndx + 1, objs2.size()), eps, context,
				code);

	}

	private void windowNestedLoop(List<VectorElemHD> objs1,
			List<VectorElemHD> objs2, double eps, Context context, long code) {

		for (int i = 0; i < objs1.size(); i++) {
			VectorElemHD outerElem = objs1.get(i);
			for (int j = 0; j < objs2.size(); j++) {
				VectorElemHD innerElem = objs2.get(j);
				// log.debug("BR SimJoin (inner): " + innerElem.toString());
//				if ((innerElem.getKey() % 2) != (outerElem.getKey() % 2)) {
					if (code == -1) {

						if (innerElem.getPrevPartition() != outerElem
								.getPrevPartition()) {
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
							for (int k = 0; k < numReduced; i++) {
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
						if (innerElem.getPrevPartition() != outerElem
								.getPrevPartition()) {
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
								for (int k = 0; k < numReduced; i++) {
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
									// innerElem.toString() + " With Distance: "
									// + diffDist);
									if (diffDist <= eps) // checks if the
									// difference in
									// distance is less
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
										// t1.toString() + " : " +
										// t2.toString());
										// log.debug("BR we found a match: " +
										// t2.toString() + " : " +
										// t1.toString());
										try {
											context.write(t1, t2); // write to
											// the
											// output
											// both
											// pairs
	//										context.write(t2, t1);
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
						}
					}// end it is a window
//				}

			}// end inner loop
		}// end outer loop

	}

	private void windowNestedLoop(List<VectorElemHD> objs, double eps,
			Context context, long code) {
		for (int i = 0; i < objs.size(); i++) {
			VectorElemHD outerElem = objs.get(i);
			// log.debug("BR SimJoin (outer): " + outerElem.toString());
			for (int j = i+1; j < objs.size(); j++) {
				VectorElemHD innerElem = objs.get(j);
				// log.debug("BR SimJoin (inner): " + innerElem.toString());
//				if ((innerElem.getKey() % 2) != (outerElem.getKey() % 2)) {
					if (code == -1) {

						if (innerElem.getPrevPartition() != outerElem
								.getPrevPartition()) {
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
							for (int k = 0; k < numReduced; i++) {
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
						if (innerElem.getPrevPartition() != outerElem
								.getPrevPartition()) {
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
								for (int k = 0; k < numReduced; i++) {
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
									// log.debug("BR Checking: Elem: " + outerElem.toString() + " Against: " + innerElem.toString() + " With Distance: " + diffDist);
									if (diffDist <= eps) // checks if the difference in distance is less than eps
									{ 
										// may be modified for more detailed
										// output?????
										Text t1 = new Text(outerElem.toString()); // set
										// up
										// output
										// for
										// joined
										// pair
										Text t2 = new Text(innerElem.toString());
										// log.debug("BR we found a match: " +
										// t1.toString() + " : " +
										// t2.toString());
										try {
											context.write(t1, t2); // write to
											// the
											// output
											// both
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
						}
					}// end it is a window
//				}
			}// end inner loop
		}// end outer loop

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
