package com.iojin.melody.mr.normal;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
import com.iojin.melody.utils.ReductionBound;
import com.iojin.melody.utils.TimerUtil;

public class NEProcessReducer extends Reducer<LongWritable, Text, LongWritable, LongWritable> {

	private int dimension = 0;
	private int numVector = 0;
	private int numBins = 0;
	private double threshold = 0.0;
	private String binPath = "";
	private String vectorPath = "";
	private double[] bins;
	private double[] vector;
	private double[] projectedBins;
	
	private double[] histA;
	private double[] histB;
	private double[] projection;
	
	private static Logger logger = Logger.getLogger(NEProcessReducer.class);
	private static long count = 0;
	private static long pruningCount = 0;
	
	private static double[] eachCombination;
	
	private int dualCounter = 0;
	private static final int numDual = 10;
	private static DualBound[] duals = new DualBound[numDual];
	private static final int numReduced = 10;
	private static final int reducedDimension = 8;
	private static ReductionBound[] reductions = new ReductionBound[numReduced];
	
	@Override
	protected void setup(Context context) throws IOException {
		logger.setLevel(Level.DEBUG);
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("dimension", 0);
		numBins = conf.getInt("bins", 0);
		threshold = conf.getFloat("threshold", 0);
		binPath = conf.get("binsPath");
		vectorPath = conf.get("vectorPath");
		
		String binsName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);
		bins = FileUtil.getFromDistributedCache(conf, binsName, numBins * dimension);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, dimension);
		projectedBins = HistUtil.projectBins(bins, dimension, vector);
		
		dimension = conf.getInt("dimension", 0);
		numBins = conf.getInt("bins", 0);
		numVector = conf.getInt("vector", 0);
		threshold = conf.getFloat("threshold", 0);
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
				logger.debug("Native for " + combination + " count: " + set.size());
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
					logger.debug("Guest for " + combination + " count: " + set.size());
				}
				
			}
		}
		logger.debug("preparation : " + (System.nanoTime() - timer) / 1000000);
		
		
		long joinTimer = System.nanoTime();
		long counter = 0;
		for (Map.Entry<String, Set<Double[]>> entry : nativeRecords.entrySet()) {
			if (guestRecords.containsKey(entry.getKey())) {
				logger.debug("To compute " + guestRecords.get(entry.getKey()).size() + " guests for combination " + entry.getKey() + " with " + entry.getValue().size() + " natives");
				TimerUtil.qnePairCounter += guestRecords.get(entry.getKey()).size() * entry.getValue().size();
				long temp = System.nanoTime();
				for (Double[] nativeRecord : entry.getValue()) {
					
					for (Double[] guestRecord : guestRecords
							.get(entry.getKey())) {
						pruningCount++;
						if (!nativeRecord.equals(guestRecord)) {
							joinRecords(nativeRecord, guestRecord, context);
							counter++;
							context.progress();
						}
					}
					
				}
				logger.debug("finished in " + (System.nanoTime() - temp)/1000000 + " ms");
			}
		}

		for (Map.Entry<String, Set<Double[]>> entry : nativeRecords.entrySet()) {
			logger.debug("To compute " + entry.getValue().size()
					+ " natives for combination " + entry.getKey());
			TimerUtil.qnePairCounter += entry.getValue().size()
					* entry.getValue().size();
			long temp = System.nanoTime();
			for (Double[] recordA : entry.getValue()) {

				for (Double[] recordB : entry.getValue()) {
					pruningCount++;
					if (compareDoubleArray(recordA, recordB)) {
						joinRecords(recordA, recordB, context);
						counter++;
						context.progress();
					}
				}
			}
			logger.debug("finished in " + (System.nanoTime() - temp) / 1000000
					+ " ms");
		}
		logger.debug("Join " + counter + " pairs , comsumed "
				+ (System.nanoTime() - joinTimer) / 1000000 + " ms");
		logger.debug("overall " + pruningCount);

	}

	private void joinRecords(Double[] recordA, Double[] recordB,Context context) throws IOException, InterruptedException {
//		long preTimer = System.nanoTime();
		long ridA = (long) recordA[0].doubleValue();
		long ridB = (long) recordB[0].doubleValue();
		for (int i = 0; i < recordA.length - 1; i++) {
			histA[i] = recordA[i + 1];
			histB[i] = recordB[i + 1];
		}
//		logger.debug("pre timer : " + (System.nanoTime() - preTimer));
		if (ridA != ridB) { // no need to compute EMD for the same record
			boolean candidate = true;
//			long pruneTimer = System.nanoTime();
			
			// projection
			for (int i = 0; i < numVector; i++) {
				for (int j = 0; j < numBins; j++) {
					projection[j] = projectedBins[i * numBins + j];
				}
				double projectEmd = DistanceUtil.get1dEmd(histA, histB, projection);
				if (projectEmd > threshold) {
					candidate = false;
					break;
				}
			}
//			logger.debug("prune timer : " + (System.nanoTime() - pruneTimer));
//			long exactTimer = System.nanoTime();
			
			if (!candidate) return;
			// dual
			if (dualCounter < numDual) {
				//System.out.print("HistA: " + FormatUtil.toString(histA));
				//System.out.println(" : HistB: " + FormatUtil.toString(histB));
				duals[dualCounter] = new DualBound(histA, histB, bins, dimension);
				dualCounter++;
			}
			for (int i = 0; i < dualCounter; i++) {
				if (duals[i].getDualEmd(histA, histB) > threshold) {
					candidate = false;
					break;
				}
			}
			
			if (!candidate) return;
			// reduced
			for (int i = 0; i < numReduced; i++) {
				if (reductions[i].getReducedEmd(histA, histB) > threshold) {
					candidate = false;
					break;
				}
			}
			
			if (!candidate) return;
			// independent minimization
			if (DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null) > threshold) {
				candidate = false;
			}			
			
			
			if (candidate) {
				double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
				count++;
				//System.out.println(ridA + " - " + ridB + ": " + emd);
				if (ridA == 116 && ridB == 548) {
					System.out.println("hist 116 " + FormatUtil.toString(histA));
					System.out.println("hist 548 " + FormatUtil.toString(histB));
				}
				if ( (emd - threshold) <= DistanceUtil.EPSILON) {
					context.write(new LongWritable(ridA), new LongWritable(ridB));
				}
			}
		}		
	}
	
	private boolean compareDoubleArray(Double[] arrayA, Double[]arrayB) {
		for (int i = 0; i < arrayA.length; i++) {
			if (arrayA[i] == arrayB[i]) {
				continue;
			}
			if (arrayA[i] <= arrayB[i]) {
				return true;
			}
			else {
				return false;
			}
		}
		return false;
	}

	
	@Override
	protected void cleanup(Context context) {
		logger.debug("Computes EMD " + count);
		logger.debug("Overall pairs on all reducers: " + TimerUtil.qnePairCounter);
	}
}
