package com.iojin.melody.mr.normal;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.iojin.melody.utils.DistanceType;
import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.DualBound;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.JoinedPair;
import com.iojin.melody.utils.ReductionBound;

public class QNEProcessRankReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	private int dimension = 0;
	private int numVector = 0;
	private int numBins = 0;
	private int paraK = 0;
	private String binPath = "";
	private String vectorPath = "";
	private double[] bins;
	private double[] vector;
	private double[] projectedBins;
	
	private double[] histA;
	private double[] histB;
	private double[] projection;
	
	private static Logger logger = Logger.getLogger(NEProcessReducer.class);
	
	private static double[] eachCombination;
	
	private int dualCounter = 0;
	private static final int numDual = 10;
	private static DualBound[] duals = new DualBound[numDual];
	private static final int numReduced = 10;
	private static final int reducedDimension = 8;
	private static ReductionBound[] reductions = new ReductionBound[numReduced];
	
	private static TreeSet<JoinedPair> joinedPairs = new TreeSet<JoinedPair>();
	
	@Override
	protected void setup(Context context) throws IOException {
		logger.setLevel(Level.DEBUG);
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("dimension", 0);
		numBins = conf.getInt("bins", 0);

		binPath = conf.get("binsPath");
		vectorPath = conf.get("vectorPath");
		
		String binsName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);
		
		numVector = conf.getInt("vector", 0);
		binPath = conf.get("binPath");
		vectorPath = conf.get("vectorPath");
		bins = FileUtil.getFromDistributedCache(conf, binsName, numBins * dimension);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, numVector * dimension);
		projectedBins = HistUtil.projectBins(bins, dimension, vector, numVector);
		histA = new double[numBins];
		histB = new double[numBins];	
		projection = new double[numBins];
		eachCombination = new double[numVector];
		
		for (int i = 0; i < numReduced; i++) {
			reductions[i] = new ReductionBound(dimension, reducedDimension, bins);
		}	

		paraK = conf.getInt("paraK", 0);
	}
	
	@Override
	protected void reduce (LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		 * prepare sets for native records and guest records
		 */
		
		long timer = System.nanoTime();
		HashMap<String, Set<Double[]>> nativeRecords = new HashMap<String, Set<Double[]>>();
		HashMap<String, Set<Double[]>> guestRecords = new HashMap<String, Set<Double[]>>();
		int offset = numBins + 2;
		for (Text val : values) {
			Double[] record = new Double[numBins + 1];
			double[] array = FormatUtil.toDoubleArray(val.toString());				
			for (int i = 0; i < numBins + 1; i++) {
				record[i] = array[i];
			}
			if (array[numBins + 1] == 0) {
				// native
				for (int i = 0; i < numVector; i++) {
					eachCombination[i] = array[offset + i];
				}
				String combination = FormatUtil.toString(eachCombination);
				if (!nativeRecords.containsKey(combination)) {
					nativeRecords.put(combination, new HashSet<Double[]>());
				}
				HashSet<Double[]> set = (HashSet<Double[]>) nativeRecords.get(combination);
				set.add(record);
				nativeRecords.put(combination, set);
			}
			
			else {
				// guest
				int numCombination = (array.length - numBins - 1) / numVector;
				
				for (int i = 0; i < numCombination; i++) {
					for (int j = 0; j < numVector; j++) {
						eachCombination[j] = array[offset + i * numVector + j];
					}
					String combination = FormatUtil.toString(eachCombination);
					if (!guestRecords.containsKey(combination)) {
						guestRecords.put(combination, new HashSet<Double[]>());
					}
					HashSet<Double[]> set = (HashSet<Double[]>) guestRecords.get(combination);
					set.add(record);
					guestRecords.put(combination, set);
				}
				
			}
		}
		logger.debug("preparation : " + (System.nanoTime() - timer) / 1000000);

		for (Map.Entry<String, Set<Double[]>> entry : nativeRecords.entrySet()) {
			if (guestRecords.containsKey(entry.getKey())) {
				for (Double[] guest : guestRecords.get(entry.getKey())) {
					for (Double[] local : entry.getValue()) {
						joinRecords(guest, local);
						context.progress();
					}
				}
			}
		}

		for (Map.Entry<String, Set<Double[]>> entry : nativeRecords.entrySet()) {
			for (Double[] recordA : entry.getValue()) {
				for (Double[] recordB : entry.getValue()) {
					joinRecords(recordA, recordB);
					context.progress();
				}
			}
		}
		
		for (JoinedPair pair : joinedPairs) {
			context.write(new LongWritable(-1), new Text(pair.toString()));
		}		
	}
	
	private void joinRecords(Double[] recordA, Double[] recordB) throws IOException, InterruptedException {
		long ridA = (long) recordA[0].doubleValue();
		long ridB = (long) recordB[0].doubleValue();
		for (int i = 0; i < recordA.length - 1; i++) {
			histA[i] = recordA[i + 1];
			histB[i] = recordB[i + 1];
		}
		if (ridA != ridB) { // no need to compute EMD for the same record
			int counter = joinedPairs.size();
			
			if (counter == paraK) {
				double kMin = joinedPairs.last().getDist();
				
				// projection
				for (int i = 0; i < numVector; i++) {
					for (int j = 0; j < numBins; j++) {
						projection[j] = projectedBins[i * numBins + j];
					}
					double projectEmd = DistanceUtil.get1dEmd(histA, histB, projection);
					if (projectEmd > kMin) {
						return;
					}
				}
			
				// dual
				if (dualCounter < numDual) {
					duals[dualCounter] = new DualBound(histA, histB, bins, dimension);
					dualCounter++;
				}
				for (int i = 0; i < dualCounter; i++) {
					if (duals[i].getDualEmd(histA, histB) > kMin) {
						return;
					}
				}
				
				// reduced
				for (int i = 0; i < numReduced; i++) {
					if (reductions[i].getReducedEmd(histA, histB) > kMin) {
						return;
					}
				}
				
				// independent minimization
				if (DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null) > kMin) {
					return;
				}			
			
			}
			
			double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
			if ( counter < paraK || emd <= joinedPairs.last().getDist()) {
				if (counter == paraK) {
					joinedPairs.remove(joinedPairs.last());
				}
				JoinedPair pair = new JoinedPair(ridA, ridB, emd);
				joinedPairs.add(pair);
			}
		}		
	}	
}
