package com.iojin.melody.bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.EmdFilter;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.JoinedPair;

public class BaselineBSP extends 
	BSP<Text, Text, Text, Text, VectorWritable>{
	
	private Configuration conf;
	private int paraK = 0;
	
	private boolean master;
	private boolean nativeDone = false;
	private int sentFlag = 0;
	private int numRecord = 0;
	private boolean receiveDone = false;
	private boolean sendDone = false;
	private TreeSet<JoinedPair> joinedPairs;
	private double beforeComputationThreshold = Double.MAX_VALUE;
	
	private int dimension;
	private int numBins;
	private int numVectors;
	private double[] bins;
	private double[] vectors;
	private EmdFilter filter;
	
	private String binPath;
	private String vectorPath;
	
	private static final double THRESHOLD_MSG = 0.0d;
	private static final double DATA_MSG = 1.0d;
	private static final double FINAL_MSG = 2.0d;
	
//	private static final int MSG_BATCH = 10;
	
	public static final String PARAK = "emd.join.para.k";
	public static final String DIMENSION = "emd.join.para.dimension";
	public static final String NUMBIN = "emd.join.num.bin";
	public static final String NUMVEC = "emd.join.num.vector";
	public static final String PATHBIN = "emd.join.path.bin";
	public static final String PATHVEC = "emd.join.path.vector";
	public static final String PATHIN = "emd.join.path.in";
	public static final String PATHOUT = "emd.join.path.out";
	
	public static final String CACHED = "emd.join.cached";
	public static final String MSG_BATCH = "emd.join.batch";
	
	// if cached
	private List<DoubleVector> cache;
	
	public static final Log LOG = LogFactory.getLog(NormalBSP.class);
	
	@Override
	public final void setup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
			throws IOException, InterruptedException {
		
		master = peer.getPeerIndex() == peer.getNumPeers() / 2;
		conf = peer.getConfiguration();
		paraK = conf.getInt(PARAK, 0);
		dimension = conf.getInt(DIMENSION, 0);
		numBins = conf.getInt(NUMBIN, 0);
		numVectors = conf.getInt(NUMVEC, 0);
		
		binPath = conf.get(PATHBIN);
		vectorPath = conf.get(PATHVEC);
		
		System.out.println(System.getProperty("java.class.path"));
		
		String binName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);
		bins = FileUtil.getFromDistributedCache(conf, binName, numBins * dimension);
		vectors = FileUtil.getFromDistributedCache(conf, vectorName, numVectors * dimension);
		
		filter = new EmdFilter(dimension, bins, vectors, null);
		
		joinedPairs = new TreeSet<JoinedPair>();
		
		if (conf.getBoolean(CACHED, true)) {
			this.cache = new ArrayList<DoubleVector>();
		}
	}
	
	@Override
	public void bsp(
			BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
			throws IOException, SyncException, InterruptedException {
		while(true) {
			process(peer);
			peer.sync();
			if (receiveDone && sendDone) {
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
	}
	
	public static BSPJob createJob(Configuration configuration, Path in, Path out) throws IOException {
		HamaConfiguration conf = new HamaConfiguration(configuration);
		BSPJob job = new BSPJob(conf, BaselineBSP.class);
		job.setJobName("Baseline Top-k Join");
		job.setJarByClass(BaselineBSP.class);
		job.setBspClass(BaselineBSP.class);
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
	
	private void mergeResult(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		VectorWritable each;
		while((each = peer.getCurrentMessage()) != null) {
			if (each.getVector().get(0) == FINAL_MSG) {
				double[] array = each.getVector().toArray();
				JoinedPair pair = BSPUtil.toJoinedPair(array);
				joinedPairs.add(pair);
				LOG.info("received " + FormatUtil.toString(each.getVector().toArray()));
			}
		}
	}
	
	private void process(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) 
			throws IOException {
		if (nativeDone) {
			guestJoin(peer);
		}
		else {
			nativeJoin(peer);
			peer.reopenInput();
		}
		
		if (receiveDone && sentFlag == numRecord) {
			sendDone = true;
			sendFinalMessage(peer);
			LOG.info("peer " + peer.getPeerIndex() + " at step " + peer.getSuperstepCount() + " sends final message");
		}
		else {
			sendMessage(peer);
		}
	}
	
	private void sendMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		
		double[] tmp = getThresholdVector();
		if (tmp != null) {
			DoubleVector thresholdVector = new DenseDoubleVector(tmp);
			for(String other : peer.getAllPeerNames()) {
				peer.send(other, new VectorWritable(thresholdVector));
			}
		}
		Text key = new Text();
		Text val = new Text();
		for (int i = 0; i < conf.getInt(MSG_BATCH, 1); i++) {
			if (cache != null && !cache.isEmpty()) {
				if (sentFlag < numRecord && peer.readNext(key, val)) {
					DoubleVector next = BSPUtil.toDoubleVector(key);
					DoubleVector dataVector = new DenseDoubleVector(getDataVector(next));
					for(String other : peer.getAllPeerNames()) {
						peer.send(other, new VectorWritable(dataVector));
					}
					sentFlag++;
				}
			}
			else if (cache == null){
				if (sentFlag < numRecord) {
					List<DoubleVector> input = readInput(peer);
					peer.reopenInput();
					DoubleVector next = input.get(sentFlag);
					DoubleVector dataVector = new DenseDoubleVector(getDataVector(next));
					for(String other : peer.getAllPeerNames()) {
						peer.send(other, new VectorWritable(dataVector));
					}
					sentFlag++;					
				}
			}
		}
	}
	
	private void sendFinalMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		if (!master) {
			for(JoinedPair pair : joinedPairs) {
				double[] each = BSPUtil.toDoubleArray(pair);
				DoubleVector finalVector = new DenseDoubleVector(getFinalVector(each));
				String theMaster = peer.getPeerName(peer.getNumPeers() / 2);
				peer.send(theMaster, new VectorWritable(finalVector));
			}
		}
	}
	
	private void nativeJoin(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {

		join(readInput(peer));
		nativeDone = true;
	}

	private void guestJoin(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
		List<DoubleVector> nativeHist = readInput(peer);
		List<DoubleVector> guestHist = new ArrayList<DoubleVector>();
		double threshold = readMessage(peer, guestHist);
		pruneLocalPairs(threshold);
		if (guestHist.isEmpty()) {
			receiveDone = true;
		}
		else {
			join(nativeHist, guestHist, threshold, peer);
		}
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
				DoubleVector each = BSPUtil.toDoubleVector(key);
				histograms.add(each);
				if (cache != null) {
					cache.add(each.deepCopy());
				}
			}
			numRecord = histograms.size();
			if (cache == null) {
				peer.reopenInput();
			}
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
			else if (each.getVector().get(0) == FINAL_MSG && master) {
				double[] array = each.getVector().toArray();
				JoinedPair pair = BSPUtil.toJoinedPair(array);
				joinedPairs.add(pair);
				LOG.info("received " + FormatUtil.toString(each.getVector().toArray()));
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
//			if (pair.getDist() >= threshold) {
			if (pair.getDist() > threshold) {
				toRemove++;
			}
		}
		for (int i = 0; i < toRemove; i++) {
			joinedPairs.pollLast();
		}
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
	
	public static void main(String args[]) {
		double[] a = {2.4118274132788766,2.894180547430983,3.698129878803758,2.974569306316426,2.974569306316426,3.456943021307981,2.1706405557830992,3.456943021307981,2.652993689935206,3.6177205390788667,2.0902517968976566,3.2157561638122036,2.4922161721643197,3.2157561638122036,2.0098486314245996,3.7785186376892015,14.72213304430151,2.632906790633569,3.2961449226976463,2.4922161721643197,3.3564467822814534,2.9946767864575112,3.0147636857591475,2.894180547430983,2.874073067289898,2.934374926873705,3.075065545342954,2.9946767864575112,2.7735974091028184,2.833878687847177};
		double[] b = {2.414153089620199,3.262383034732963,3.392869670375926,3.0013930600886614,2.414153089620199,3.197123013553105,3.392869670375926,3.392869670375926,3.262383034732963,2.414153089620199,2.3489097717987177,3.197123013553105,2.60989974644302,2.805646403265841,3.0013930600886614,3.0013930600886614,14.721471499059602,2.675143064264501,2.9198305611326214,3.0340147189994022,3.1481988768661826,3.0176955378648427,3.0992580368208835,2.805646403265841,2.854570539952763,2.8219488810420224,3.2950046936437043,2.7730247443551,2.887208902221881,2.8382680621765815};
		double[] bins = {0,0,0,0,1,0,0,2,0,0,3,0,1,0,0,1,1,0,1,2,0,1,3,0,2,0,0,2,1,0,2,2,0,2,3,0,3,0,0,3,1,0,3,2,0,3,3,0,1.5,1.5,2,1.5,0,1,1.5,1,1,1.5,2,1,1.5,3,1,0,1.5,1,1,1.5,1,2,1.5,1,3,1.5,1,0.5,0.5,1,2,0.5,1,0.5,2,1,2,2,1,1.5,1.5,1};
		int dimension = 3;
		System.out.println(DistanceUtil.getEmdLTwo(a, b, dimension, bins));
	}
}
