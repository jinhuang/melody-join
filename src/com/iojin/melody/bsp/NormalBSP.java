package com.iojin.melody.bsp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.KeyValueTextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;
import org.apache.hama.ml.writable.VectorWritable;

import com.iojin.melody.utils.BSPUtil;
import com.iojin.melody.utils.Direction;
import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.EmdFilter;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.JoinedPair;
import com.iojin.melody.utils.QuantileGrid;

public class NormalBSP extends 
	BSP<Text, Text, Text, Text, VectorWritable>{
	
	private Configuration conf;
	private int paraK = 0; 
	
	private boolean master;
	private int assignmentId = -1;
	private boolean nativeDone = false;
	private int sentFlag = 0;
	private int numRecord = 0;
	private boolean sendDone = false;
	private boolean finalDone = false;
	private TreeSet<JoinedPair> joinedPairs;
	private double beforeComputationThreshold = Double.MAX_VALUE;
	
	private int dimension;
	private int numBins;
	private int numVectors;
	private int numGrid;
//	private int numDual;
	private double[] bins;
	private double[] vectors;
	private double[] projectedBins;
	private EmdFilter filter;
	private int seenSendDone = 0;
	private int seenFinal = 0;
	
	Map<String, Double[][]> cellError = new HashMap<String, Double[][]>();
	Map<String, Double[]> cellRubner = new HashMap<String, Double[]>();
	Map<String, Double[]> cellDual = new HashMap<String, Double[]>();
	Map<Integer, List<String>> cellWorkload = new HashMap<Integer, List<String>>(); // assigmentId, cellId
	Map<String, Integer> cellPeer = new HashMap<String, Integer>(); // peerName, assignmentId
	private QuantileGrid[] grids;
	private double[] t;
	
	private String binPath;
	private String vectorPath;
	
	private static final double PEERACK_MSG = -1.0d;
	private static final double THRESHOLD_MSG = 0.0d;
	private static final double DATA_MSG = 1.0d;
	private static final double FINAL_MSG = 2.0d;
	private static final double SENDACK_MSG = 3.0d;
	private static final double FINALACK_MSG = 4.0d;
	
	public static final String PARAK = "emd.join.para.k";
	public static final String DIMENSION = "emd.join.para.dimension";
	public static final String NUMTASK = "emd.join.num.task";
	public static final String NUMBIN = "emd.join.num.bin";
	public static final String NUMVEC = "emd.join.num.vector";
	public static final String NUMINTERVAL = "emd.join.num.interval";
	public static final String NUMDUAL = "emd.join.num.dual";
	public static final String NUMGRID = "emd.join.num.grid";
	public static final String LENGTHERROR = "emd.join.length.error";
	public static final String PATHBIN = "emd.join.path.bin";
	public static final String PATHVEC = "emd.join.path.vector";
	public static final String PATHIN = "emd.join.path.in";
	public static final String PATHPREIN = "emd.join.path.preparein";
	public static final String PATHCELL = "emd.join.path.cell";
	public static final String PATHGRID = "emd.join.path.grid";
	public static final String PATHOUT = "emd.join.path.out";
	
	public static final String CACHED = "emd.join.cached";
	public static final String MSG_BATCH = "emd.join.batch";
	
	public static final Log LOG = LogFactory.getLog(NormalBSP.class);
	public static long joinTimer = 0;
	public static long filterTimer = 0;
	
	// if cached
	private List<DoubleVector> cache;
	private List<String> cacheCell;
	
	@Override
	public final void setup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
			throws IOException, InterruptedException {
		LOG.info("initiating peer " + peer.getPeerIndex());
		master = peer.getPeerIndex() == peer.getNumPeers() / 2;
		conf = peer.getConfiguration();
		paraK = conf.getInt(PARAK, 0);
		dimension = conf.getInt(DIMENSION, 0);
		numBins = conf.getInt(NUMBIN, 0);
		numVectors = conf.getInt(NUMVEC, 0);
		numGrid = conf.getInt(NUMGRID, 0);
//		numDual = conf.getInt(NUMDUAL, 0);
		grids = new QuantileGrid[numVectors];
		
		
		binPath = conf.get(PATHBIN);
		vectorPath = conf.get(PATHVEC);
		
		System.out.println(System.getProperty("java.class.path"));
		
		String binName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);
		bins = FileUtil.getFromDistributedCache(conf, binName, numBins * dimension);
		vectors = FileUtil.getFromDistributedCache(conf, vectorName, numVectors * dimension);
		projectedBins = HistUtil.projectBins(bins, dimension, vectors, numVectors);
		
		t = new double[numVectors * 2];
		for (int i = 0; i < numVectors; i++) {
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			t[i * 2] += HistUtil.getMinIn(eachProjection) - FormatUtil.avg(eachProjection);	
			t[i * 2 + 1] += HistUtil.getMaxIn(eachProjection) - FormatUtil.avg(eachProjection);			
		}
		
		filter = new EmdFilter(dimension, bins, vectors, null);
		
		joinedPairs = new TreeSet<JoinedPair>();
		
		readCell();
		readGrid();
		
		if (conf.getBoolean(CACHED, true)) {
			this.cache = new ArrayList<DoubleVector>();
			this.cacheCell = new ArrayList<String>();
		}
		
		LOG.info("starting up peer " + peer.getPeerIndex());
	}
	
	@Override
	public void bsp(
			BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
			throws IOException, SyncException, InterruptedException {
		while(true) {
//			LOG.info("peer " + peer.getPeerIndex() + " sc: " + peer.getSuperstepCount());
			process(peer);
			peer.sync();
			if (seenSendDone == peer.getNumPeers() && seenFinal == peer.getNumPeers()) {
				if (master) {
					mergeResult(peer);
				}
				break;
			}
		}
	}
	
	@Override
	public final void cleanup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		if (master) {
			if (joinedPairs.size() >= paraK) {
				for (int i = 0; i < paraK; i++) {
					JoinedPair pair = joinedPairs.pollFirst();
					peer.write(new Text(String.valueOf(i)), new Text(pair.toString()));
				}
			}
			else peer.write(new Text("Error"), new Text(String.valueOf(beforeComputationThreshold)));
		}
		double sec = joinTimer / 1000000000.0d;
		LOG.info("Guest join takes " + sec + " seconds");
		sec = filterTimer / 1000000000.0d;
		LOG.info("Filter takes " + sec + " seconds");
	}
	
	public static BSPJob createJob(Configuration configuration, Path in, Path out) throws IOException {
		HamaConfiguration conf = new HamaConfiguration(configuration);
		BSPJob job = new BSPJob(conf, NormalBSP.class);
		job.setJobName("Normal Top-k Join");
		job.setJarByClass(NormalBSP.class);
		job.setBspClass(NormalBSP.class);
		job.setInputPath(in);
		job.setOutputPath(out);
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setInputKeyClass(VectorWritable.class);
		job.setInputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job;
	}
	
	public void mergeResult(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		VectorWritable each;
		while((each = peer.getCurrentMessage()) != null) {
			if (each.getVector().get(0) == FINAL_MSG) {
				double[] array = each.getVector().toArray();
				JoinedPair pair = BSPUtil.toJoinedPair(array);
				joinedPairs.add(pair);
			}
		}
	}
	
	public void process(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) 
			throws IOException {
		if (nativeDone) {
			guestJoin(peer);
		}
		else {
			nativeJoin(peer);
			peer.reopenInput();
		}
		
		if (sentFlag == numRecord && !sendDone) {
			sendDone = true;
			sendSeenMessage(peer);
		}
		else if(sentFlag == numRecord && sendDone && !finalDone) {
			finalDone = true;
			sendFinalMessage(peer);
		}
		else {
			sendMessage(peer);
		}
		
//		peer.write(new Text(String.valueOf(peer.getSuperstepCount())), new Text("Send: " + seenSendDone + " ; Final " + seenFinal));
		LOG.info(peer.getSuperstepCount() + ": send " + seenSendDone + ", final " + seenFinal);
	}
	
	private void sendMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		
		if (cellPeer.isEmpty()) {
			double[] tmp = getPeerAckVector(peer);
			for (String other : peer.getAllPeerNames()) {
				if (!other.equals(peer.getPeerName())) {
					peer.send(other, new VectorWritable(new DenseDoubleVector(tmp)));
				}
			}
			cellPeer.put(peer.getPeerName(peer.getPeerIndex()), assignmentId);
		}
		else {
			double[] tmp = getThresholdVector();
			if (tmp != null) {
				DoubleVector thresholdVector = new DenseDoubleVector(tmp);
				for(String other : peer.getAllPeerNames()) {
					if (!other.equals(peer.getPeerName())) {
						peer.send(other, new VectorWritable(thresholdVector));
					}
				}
			}
			Text key = new Text();
			Text val = new Text();
			for (int i = 0; i < conf.getInt(MSG_BATCH, 1); i++) {
//				if (cache != null && !cache.isEmpty()) {
					if (sentFlag < numRecord && peer.readNext(key, val)) {
						String[] split = key.toString().split(";");
						assignmentId = Integer.valueOf(split[0]);
						DoubleVector next = BSPUtil.toDoubleVector(split[2]);
						DoubleVector dataVector = new DenseDoubleVector(getDataVector(next));
						for(String other : peer.getAllPeerNames()) {
							if (!other.equals(peer.getPeerName())) {
								if (!filter(other, next, split[1], beforeComputationThreshold, peer)) {
									peer.send(other, new VectorWritable(dataVector));
								}
							}
						}
						sentFlag++;
					}
			}
		}
	}
	
	private boolean filter(String cellName, DoubleVector data, String id, double threshold, BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		long start = System.nanoTime();
		double[] hist = data.toArray();
		double[] weight = HistUtil.normalizeArray(FormatUtil.getSubArray(hist, 1, hist.length - 1));
		List<String> cellIds = cellWorkload.get(cellPeer.get(cellName));
		double[] record = new double[2];
		int lengthError = conf.getInt(NormalBSP.LENGTHERROR, 0);
		int numInterval = conf.getInt(NormalBSP.NUMINTERVAL, 0);
		double[] error = new double[lengthError];
		for (String cellId : cellIds) {
			if (cellId.equals(id)) continue;
			String[] eachCellIds = cellId.split(",");
			double maxEmdLB = -Double.MAX_VALUE;
//			// emdbr
			for (int v = 0; v < numVectors; v++) {
				double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, v);
				eachProjection = FormatUtil.substractAvg(eachProjection);
				NormalDistributionImpl normal = HistUtil.getNormal(weight, eachProjection);
				TreeMap<Double, Double> cdf = HistUtil.getDiscreteCDFNormalized(weight, eachProjection);
				List<Double> errors = HistUtil.getMinMaxError(normal, cdf, numInterval);
				for (int j = 0; j < errors.size(); j++) {
					error[j] = errors.get(j);
				}
				double fullError = HistUtil.getFullError(normal, cdf, t[v*2], t[v*2+1]);
				error[lengthError - 1] = fullError;
				record[0] = 1 / normal.getStandardDeviation();
				record[1] = (-1) * normal.getMean() / normal.getStandardDeviation();
				
				int eachId = Integer.valueOf(eachCellIds[v]);
				double[] bound = grids[v].getGridBound(eachId);
				Direction direction = grids[v].locateRecordToGrid(record, bound);
				double emdBr = grids[v].getEmdBr(record, error, bound, FormatUtil.toDoubleArray(cellError.get(cellId)[v]), direction,numInterval);
				maxEmdLB = maxEmdLB > emdBr ? maxEmdLB : emdBr;
			}
//			// rubner
			double[] rubnerValue = DistanceUtil.getRubnerValue(weight, dimension, bins);
			double rubnerEmd = DistanceUtil.getRubnerBound(rubnerValue, FormatUtil.toDoubleArray(cellRubner.get(cellId)), dimension);
			maxEmdLB = maxEmdLB > rubnerEmd ? maxEmdLB : rubnerEmd;
			// dual
//			for (int i = 0; i < numDual; i++) {
//				
//			}
			if (maxEmdLB < threshold) {
				return false;
			}
		}
		filterTimer += System.nanoTime() - start;
		return true;
	}
	
	
	private void sendFinalMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
//		if (!master) {
			for(JoinedPair pair : joinedPairs) {
				double[] each = BSPUtil.toDoubleArray(pair);
				DoubleVector finalVector = new DenseDoubleVector(getFinalVector(each));
//				String theMaster = peer.getPeerName(peer.getNumPeers() / 2);
				for (String other : peer.getAllPeerNames()) {
					if (!other.equals(peer.getPeerName())) {
						peer.send(other, new VectorWritable(finalVector));
					}
				}
				
			}
//			peer.write(new Text("done!"), new Text(String.valueOf(beforeComputationThreshold)));
//			seenFinal++;
			for (String other : peer.getAllPeerNames()) {
//				if (!other.equals(peer.getPeerName())) {
					peer.send(other, new VectorWritable(new DenseDoubleVector(getFinalAckVector())));
//				}
			}			
			LOG.info("peer " + peer.getPeerIndex() + " sent final message with seenFinal " + seenFinal);
	}
	
	private void sendSeenMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		DoubleVector seenVector = new DenseDoubleVector(getSendDoneVector());
		for (String other : peer.getAllPeerNames()) {
			if (!other.equals(peer.getPeerName())) {
				peer.send(other, new VectorWritable(seenVector));
			}
		}
		seenSendDone++;
	}
	
	private void nativeJoin(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {

		join(readInput(peer));
		nativeDone = true;
	}

	private void guestJoin(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		long start = System.nanoTime();
		List<DoubleVector> nativeHist = readInput(peer);
		List<DoubleVector> guestHist = new ArrayList<DoubleVector>();
		double threshold = readMessage(peer, guestHist);
		pruneLocalPairs(threshold);
//		if (guestHist.isEmpty()) {
//			receiveDone = true;
//		}
		if (!guestHist.isEmpty()){
			join(nativeHist, guestHist, threshold, peer);
		}
		joinTimer += System.nanoTime() - start;
	}
	
	private void join(List<DoubleVector> histograms) {
		double threshold = getLocalThreshold();
		for (int i = 0; i < histograms.size(); i++) {
			double[] recordA = histograms.get(i).toArray();
			long rid = (long) recordA[0];
			double[] weightA = HistUtil.normalizeArray(FormatUtil.getSubArray(recordA, 1, recordA.length - 1));
			for (int j = i + 1; j < histograms.size(); j++) {
				double[] recordB = histograms.get(j).toArray();
				long sid = (long) recordB[0];
				double[] weightB = HistUtil.normalizeArray(FormatUtil.getSubArray(recordB, 1, recordB.length - 1));
				
				if (!filter.filter(weightA, weightB, threshold)) {
					double emd = DistanceUtil.getEmdLTwo(weightA, weightB, dimension, bins);
					if (emd < threshold) {
						JoinedPair pair = new JoinedPair(rid, sid, emd);
						joinedPairs.add(pair);
						if (joinedPairs.size() > paraK) {
							joinedPairs.pollLast();
						}
						threshold = getLocalThreshold();
					}
				}
			}
		}
	}
	
	private void join(List<DoubleVector> nat, List<DoubleVector> gus, double t, BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
	
		double threshold = getLocalThreshold(t);
		for (int i = 0; i < nat.size(); i++) {
			double[] recordA = nat.get(i).toArray();
			long rid = (long) recordA[0];
			double[] weightA = HistUtil.normalizeArray(FormatUtil.getSubArray(recordA, 1, recordA.length - 1));
			for (int j = 0; j < gus.size(); j++) {
				double[] recordB = gus.get(j).toArray();
				long sid = (long) recordB[0];
				double[] weightB = HistUtil.normalizeArray(FormatUtil.getSubArray(recordB, 1, recordB.length - 1));
				if ((rid + sid) % 2 == 0 && rid < sid
						|| (rid + sid) % 2 == 1 && rid > sid) {
					if (!filter.filter(weightA, weightB, threshold)) {
						double emd = DistanceUtil.getEmdLTwo(weightA, weightB, dimension, bins);
//						peer.write(new Text("join "), new Text(rid + " " + sid + " : " + emd));
						if (emd < threshold) {
							JoinedPair pair = new JoinedPair(rid, sid, emd);
							joinedPairs.add(pair);
							if (joinedPairs.size() > paraK) {
								joinedPairs.pollLast();
							}
							threshold = getLocalThreshold(t);
						}
					}
				}
			}
		}

	}
	
	private List<DoubleVector> readInput(BSPPeer<Text, Text,  Text, Text, VectorWritable> peer) throws IOException {
		if (cache != null && cache.isEmpty() || cache == null) {
			List<DoubleVector> histograms = new ArrayList<DoubleVector>();
			Text key = new Text();
			Text val = new Text();
			while (peer.readNext(key, val)) {
				String[] split = key.toString().split(";");
				assignmentId = Integer.valueOf(split[0]);
				DoubleVector each = BSPUtil.toDoubleVector(split[2]);
				histograms.add(each);
				if (cache != null) {
					cache.add(each.deepCopy());
				}
				if (cacheCell != null) {
					cacheCell.add(split[1]);
				}
			}
			numRecord = histograms.size();
			return histograms;
		}
		else return cache;
	}
	
	private double readMessage(BSPPeer<Text, Text,  Text, Text, VectorWritable> peer,
			List<DoubleVector> guestHist) throws IOException {
//		double threshold = getLocalThreshold();
		double threshold = beforeComputationThreshold;
		VectorWritable each;
		DoubleVector hist;
		while((each = peer.getCurrentMessage()) != null) {
			if (each.getVector().get(0) == THRESHOLD_MSG) {
				double eachThreshold = each.getVector().get(1);
				threshold = eachThreshold < threshold ? eachThreshold : threshold;	
			}
			else if (each.getVector().get(0) == DATA_MSG){
				hist = each.getVector().slice(1, each.getVector().getDimension());
				guestHist.add(hist.deepCopy());
			}	
			else if (each.getVector().get(0) == FINAL_MSG) {
				if (master) {
					double[] array = each.getVector().toArray();
					JoinedPair pair = BSPUtil.toJoinedPair(array);
					joinedPairs.add(pair);
				}
				
			}
			else if (each.getVector().get(0) == FINALACK_MSG) {
				seenFinal++;
				LOG.info("peer " + peer.getPeerIndex() + " " + seenFinal);
			}
			else if (each.getVector().get(0) == SENDACK_MSG) {
				seenSendDone++;
			}
			else if (each.getVector().get(0) == PEERACK_MSG) {
				double[] array = each.getVector().toArray();
				int eachId = (int) array[1];
				String name = peer.getPeerName((int) array[2]);
				cellPeer.put(name, eachId);
			}
		}
		beforeComputationThreshold = threshold;
		return threshold;
	}
	
	private double getLocalThreshold() {
		double threshold = Double.MAX_VALUE;
		if (joinedPairs.size() == paraK) {
			threshold = joinedPairs.last().getDist();
		}
		return threshold;
	}
	
	private double getLocalThreshold(double g) {
		double threshold = g;
		if (joinedPairs.size() == paraK) {
			double local = joinedPairs.last().getDist();
			threshold = local < g ? local : g;
		}
		return threshold;
	}
	
	private void pruneLocalPairs(double threshold) {
		int toRemove = 0;
		for (JoinedPair pair : joinedPairs) {
			if (pair.getDist() > threshold) {
				toRemove++;
			}
		}
		for (int i = 0; i < toRemove; i++) {
			joinedPairs.pollLast();
		}
	}
	
	private double[] getPeerAckVector(BSPPeer<Text, Text,  Text, Text, VectorWritable> peer) {
		double[] result = new double[3];
		result[0] = PEERACK_MSG;
		result[1] = assignmentId;
		result[2] = peer.getPeerIndex();
		return result;
	}
	
	private double[] getThresholdVector() {
		if (joinedPairs.isEmpty()) return null;
		double threshold = joinedPairs.last().getDist();
		if (joinedPairs.size() == paraK && threshold < beforeComputationThreshold) {
			double[] vector = new double[2];
			vector[0] = THRESHOLD_MSG;
			vector[1] = threshold;
			return vector;
		}
		else return null;
	}
	
	private double[] getDataVector(DoubleVector hist) {
		double[] data = hist.toArray();
		double[] vector = new double[data.length + 1];
		vector[0] = DATA_MSG;
		for (int i = 1; i <= data.length; i++) {
			vector[i] = data[i - 1];
		}
		return vector;
	}
		
	private double[] getFinalVector(double[] each) {
		double[] result = new double[each.length + 1];
		result[0] = FINAL_MSG;
		for (int i = 1; i < result.length; i++) {
			result[i] = each[i - 1];
		}
		return result;
	}
	
	private double[] getSendDoneVector() {
		double[] tmp = new double[1];
		tmp[0] = SENDACK_MSG;
		return tmp;
	}
	
	private double[] getFinalAckVector() {
		double[] tmp = new double[1];
		tmp[0] = FINALACK_MSG;
		return tmp;
	}
	
	private void readCell() throws IOException {
		String path = conf.get(NormalBSP.PATHCELL);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(path));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		int numVectors = conf.getInt(NormalBSP.NUMVEC, 0);
		int errorLength = conf.getInt(NormalBSP.LENGTHERROR, 0) + 1;
		while((line = reader.readLine()) != null) {
			String[] split = line.split(";");
			String cell = split[0];
			int assignmentId = Integer.valueOf(split[1]);
			if (!cellWorkload.containsKey(assignmentId)) {
				cellWorkload.put(assignmentId, new ArrayList<String>());
			}
			cellWorkload.get(assignmentId).add(cell);
			Double[][] error = new Double[numVectors][errorLength];
			for (int i = 0; i < numVectors; i++) {
				error[i] = FormatUtil.toObjectDoubleArray(split[2 + i]);
			}
			cellError.put(cell, error);
			Double[] rubner = FormatUtil.toObjectDoubleArray(split[2 + numVectors]);
			cellRubner.put(cell, rubner);
			Double[] dual = FormatUtil.toObjectDoubleArray(split[2 + numVectors + 1]);
			cellDual.put(cell, dual);
		}
	}
	
	private void readGrid() throws IOException {
		String path = conf.get(NormalBSP.PATHGRID);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(path));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		int i = 0;
		while((line = reader.readLine()) != null) {
			String[] split = line.split(";");
			double[] domain = FormatUtil.toDoubleArray(split[1]);
			double[] slope = FormatUtil.toDoubleArray(split[2]);
			double[] swQ = FormatUtil.toDoubleArray(split[3]);
			double[] seQ = FormatUtil.toDoubleArray(split[4]);
			grids[i] = new QuantileGrid(domain, slope, numGrid, swQ, seQ);
			i++;
		}		
	}
}
