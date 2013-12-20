package com.iojin.melody.mr.normal;

import java.io.IOException;
import java.util.HashMap;
//import java.util.HashSet;
import java.util.List;
//import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.iojin.melody.utils.Candidate;
import com.iojin.melody.utils.DualBound;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.Grid;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.QuantileGrid;
import com.iojin.melody.utils.TimerUtil;

public class QNEProcessMapper extends Mapper<Object, Text, LongWritable, Text> {
	
	private static final int ntv = 0;
	private static final int guest = 1;
	
	private int grid = 0;
	private int numInterval = 0;
	private int dimension = 0;
	private int numVector = 0;
	private int numBins = 0;
	private double threshold = 0.0;
	private int paraK = 0;
	private int worker = 0;
	private String binPath = "";
	private String vectorPath = "";
//	private String tPath = "";
	private double[] bins;
	private double[] vector;
	private double[] projectedBins;
	private double[] t;
	private static double[] quantileArray;
	private static double[] quantile;
	private double[] error;
	private HashMap<String, Long> assignment;
	private Grid[] grids;
	private long[] gridIds;
	private static String[] workerCombinations;
	private static Logger logger = Logger.getLogger(QNEProcessMapper.class);
	private static LongWritable outKey = new LongWritable(0);
	private static Text outVal = new Text();
	private static long timer = 0;
	
	private static String queryType;
	private static List<Double[]> references;
	private static int numReference;
	
	private static double globalUpper = Double.MAX_VALUE;
	
	private static TreeSet<Candidate> rankTmpCandidates = new TreeSet<Candidate>();
	private static TreeSet<Candidate> rankCandidates = new TreeSet<Candidate>();
//	private static Set<Long> rankWrittenIds = new HashSet<Long>();
	
	/*
	 * duals and dual keys for combinations
	 */
	private static DualBound[] duals;
	private static HashMap<String, Double[]> dualKeys = new HashMap<String, Double[]>();
	
	private static HashMap<String, Double[]> rubnerKeys = new HashMap<String, Double[]>();
	
	@Override
	protected void setup(Context context) throws IOException {
		logger.setLevel(Level.WARN);
		long setupTimer = System.nanoTime();
		
		Configuration conf = context.getConfiguration();
		grid = conf.getInt("grid", 0);
		numInterval = conf.getInt("interval", 5);
		dimension = conf.getInt("dimension", 0);
		numBins = conf.getInt("bins", 0);
		numVector = conf.getInt("vector", 0);
		threshold = conf.getFloat("threshold", 0);
		
		worker = conf.getInt("worker", 0);
		binPath = conf.get("binsPath");
		vectorPath = conf.get("vectorPath");
//		tPath = conf.get("tPath");
		queryType = conf.get("query");
		if (queryType.equals("rank") || queryType.equals("knn")) {
			String referencePath = conf.get("referencePath");
			references = FileUtil.readMultiFromHDFS(conf, referencePath);
			numReference = references.size();
			paraK = conf.getInt("threshold", 0);
		}		
		System.out.println("parameter k : " + paraK);
		
		String binsName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);
//		String tName = FileUtil.getNameFromPath(tPath);
		String domainName = "domain";
		String errorName = "error";
		bins = FileUtil.getFromDistributedCache(conf, binsName, numBins * dimension);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, numVector * dimension);
		projectedBins = HistUtil.projectBins(bins, dimension, vector, numVector);
//		t = FileUtil.getFromDistributedCache(conf, tName, numVector * 2);
		t = new double[2 * numVector];
		for (int i = 0; i < numVector; i++) {
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			t[i * 2] += HistUtil.getMinIn(eachProjection) - HistUtil.avg(eachProjection);	
			t[i * 2 + 1] += HistUtil.getMaxIn(eachProjection) - HistUtil.avg(eachProjection);			
		}		
		
		quantileArray = FileUtil.getMultiFromDistributedCache(conf, domainName, 1 + (grid + 3) * 2);
		//logger.debug("Read quantile: " + FormatUtil.toString(quantileArray));
		quantile = FormatUtil.omitVectorId(quantileArray, 1 + (grid + 3) * 2);
		//logger.debug("Converted quantile: " + FormatUtil.toString(quantile));
		double [] slopes = new double[2 * numVector];
		//logger.debug("Read t: " + FormatUtil.toString(t));
		for (int i = 0; i < numVector; i++) {
			slopes[i*2] = (-1) * t[i*2 + 1];
			slopes[i*2+1] = (-1) * t[i*2];
		}
		//logger.debug("Convered slopes: " + FormatUtil.toString(slopes)); 
		grids = new QuantileGrid[numVector];
		for (int i = 0; i < numVector; i++) {
			double[] each = FormatUtil.getNthSubArray(quantile, 4 + 2*(grid + 1), i);
			double[] eachDomain = FormatUtil.getSubArray(each, 0, 3);
			double[] eachSlopes = FormatUtil.getNthSubArray(slopes, 2, i);
			double[] eachSW = FormatUtil.getSubArray(each, 4, 3 + (grid + 1));
			double[] eachSE = FormatUtil.getSubArray(each, 4 + grid + 1, 3 + 2 * (grid + 1));
			grids[i] = new QuantileGrid(eachDomain, eachSlopes, grid, eachSW, eachSE);
		}
		int eachCombinationLength = numVector * (2 * (numInterval + 1)) + 1 + numVector;
		if (queryType.equals("knn") || queryType.equals("rank")) {
			eachCombinationLength += numReference;
		}
		
		error = FileUtil.getMultiFromDistributedCache(conf, errorName, eachCombinationLength);
		//logger.debug("Error read: " + FormatUtil.toString(error)); 
		HashMap<String, Long> count = new HashMap<String, Long>();
		int combinationNum = error.length / eachCombinationLength;
		for (int i = 0; i < combinationNum; i++) {
			double[] eachCombination = FormatUtil.getNthSubArray(error, eachCombinationLength, i);
			double[] ids = FormatUtil.getSubArray(eachCombination, 0, numVector-1);
			long[] idsLong = new long[ids.length];
			for (int j = 0; j < ids.length; j++) {
				idsLong[j] = Math.round(ids[j]);
			}
			count.put(FormatUtil.formatCombination(idsLong), Math.round(eachCombination[numVector]));
		}
		
//		for (String key : count.keySet()) {
//			System.out.println("combination " + key + " " + count.get(key));
//		}
 		
		assignment = Grid.assignGrid(count, worker);
		
		
		int[] workerCount = new int[worker];
		for (String combination : assignment.keySet()) {
			workerCount[(int)assignment.get(combination).longValue()] += count.get(combination);
		}
		
		for (int i = 0; i < worker; i++) {
			System.out.println("Worker " + i + " " + workerCount[i]);
		}
		
		workerCombinations = new String[worker];
		
		gridIds = new long[numVector];
		
		
		/*
		 * read duals
		 */
		List<Double[]> dualSolutions = FileUtil.readMultiFromHDFS(conf, conf.get("dualPath"));
		duals = new DualBound[dualSolutions.size()];
		int counter = 0;
		for(Double[] solution : dualSolutions) {
			String solutionString = FormatUtil.toTextString(solution);
			duals[counter] = new DualBound(solutionString);
			counter++;
		}			
		
		/*
		 * read dual keys for combination
		 */
		int eachKeyLength = numVector + duals.length * 2;
		List<Double[]> keysList = FileUtil.readMultiFromHDFS(conf, conf.get("keyPath"));
		//double[] keys = FileUtil.getMultiFromDistributedCache(conf, "key", eachKeyLength);
		double[] keys = FormatUtil.listToOneArray(keysList);
		double[] eachKey = new double[eachKeyLength];
		
		for (int i = 0; i < keys.length / eachKeyLength; i++) {
			Double[] tmp = new Double[duals.length * 2];
			eachKey = FormatUtil.getNthSubArray(keys, eachKeyLength, i, eachKey);
			double[] combination = FormatUtil.getSubArray(eachKey, 0, numVector - 1);
			double[] minmax = FormatUtil.getSubArray(eachKey, numVector, eachKeyLength- 1);
			for (int j = 0; j < tmp.length ;j++) {
				tmp[j] = minmax[j];
			}
			long[] combinationIds = new long[combination.length];
			for (int j = 0; j < combination.length; j++) {
				combinationIds[j] = (long) combination[j];
			}
			dualKeys.put(FormatUtil.formatCombination(combinationIds), tmp);
		}
		
		int eachRubnerKeyLength = numVector + dimension * 2;
		List<Double[]> rubnerList = FileUtil.readMultiFromHDFS(conf, conf.get("rubnerPath"));
		double[] eachRubnerKey = new double[eachRubnerKeyLength];
		for (int i = 0; i < rubnerList.size(); i++) {
			eachRubnerKey = FormatUtil.toDoubleArray(rubnerList.get(i));
			double[] combination = FormatUtil.getSubArray(eachRubnerKey, 0, numVector - 1);
			double[] minmax = FormatUtil.getSubArray(eachRubnerKey, numVector, eachRubnerKeyLength - 1);
			long[] combinationIds = new long[combination.length];
			for (int j = 0; j < combination.length; j++) {
				combinationIds[j] = (long) combination[j];
			}
			rubnerKeys.put(FormatUtil.formatCombination(combinationIds), FormatUtil.toObjectDoubleArray(minmax));
		}
		
		if (queryType.equals("rank")) {
			globalUpper = Double.valueOf(FileUtil.readSingleFromHDFS(conf, conf.get("rankPath"))); 
		}
		
		logger.debug("Setup: " + (System.nanoTime() - setupTimer) / 1000000 + "ms");
	}
	
	@Override
	protected void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		
		String recordString = values.toString();
		long recordId = (long)(FormatUtil.toDoubleArray(recordString)[0]);		
		
		/*
		 * get weights in bins from the second value (the first is the id)
		 */		
		double [] weights = FormatUtil.getDoubleArray(values, 1, numBins);
		if (recordId == 116) {
			System.out.println(FormatUtil.toString(weights));
		}
		weights = HistUtil.normalizeArray(weights);
		if (recordId == 116) {
			System.out.println(FormatUtil.toString(weights));
		}
		
		int errorLength = 2 * numInterval + 1;
		double[] recordErrors = new double[errorLength * numVector];
		double[] recordLocations = new double[2 * numVector];
		
//		long recordTimer = System.nanoTime();
		for (int i = 0; i < numVector; i++) {
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			eachProjection = HistUtil.substractAvg(eachProjection);
			/*
			 * approximate the projected histogram using a normal distribution
			 */		
			NormalDistributionImpl normal = HistUtil.getNormal(weights, eachProjection);
			
			/*
			 * get discrete CDF
			 */		
			TreeMap<Double, Double> cdf = HistUtil.getDiscreteCDFNormalized(weights, eachProjection);
//			TreeMap<Double, Double> cdf = HistUtil.getDiscreteCDF(weights, eachProjection);
			
			/*
			 * get approximation errors of normal distribution towards discrete cdf
			 */		
			List<Double> errors = HistUtil.getMinMaxError(normal, cdf, numInterval);
			for (int j = 0; j < errors.size(); j++) {
				recordErrors[i * errorLength + j] = errors.get(j);
			}
			double fullError = HistUtil.getFullError(normal, cdf, t[i*2], t[i*2+1]);
			recordErrors[(i + 1) * errorLength + -1] = fullError;
			//logger.debug("record error " + FormatUtil.toString(FormatUtil.getSubArray(recordErrors, i * errorLength, (i+1) * errorLength - 1)));
			
			/*
			 * transform to Hough space
			 */
			recordLocations[i * 2] = 1 / normal.getStandardDeviation();
			recordLocations[i * 2 + 1] = (-1) * normal.getMean() / normal.getStandardDeviation();
			
			/*
			 * locate the record in a grid in the transformed space
			 */		
			gridIds[i] = grids[i].getGridId(FormatUtil.getSubArray(recordLocations, i * 2, i * 2 + 1));	
		}
		String combination = FormatUtil.formatCombination(gridIds);
		
//		logger.debug("record computed: " + (System.nanoTime() - recordTimer)/1000000 + "ms");
		
		long guestTimer = System.nanoTime();
//		List<String> guestCombination = Grid.getGuestForCombination(recordLocations, recordErrors, combination,
//				threshold, error, numInterval, numVector, grids, recordId);
		List<String> guestCombination = null;
		
		if (queryType.equals("rank")) {
			TreeSet<Candidate> candidates = Grid.getGuestForRank(recordLocations, recordErrors, combination,
					threshold, error, numInterval, numVector, grids, recordId, duals, dualKeys, weights,
					numReference, paraK, references, bins, dimension, globalUpper, rubnerKeys);

			for (Candidate each : candidates) {
				each.setRid(recordId);
				each.setWeights(weights);
				each.setNativeGrids(FormatUtil.formatCombination(gridIds));
			}
			
			rankTmpCandidates.clear();
			rankTmpCandidates.addAll(rankCandidates);
			rankTmpCandidates.addAll(candidates);
			globalUpper = Grid.pruneCandidates(rankTmpCandidates, rankCandidates, paraK, globalUpper);
			TimerUtil.rankTimer += System.nanoTime() - guestTimer;
	
		}
		
		else {
			guestCombination = Grid.getGuestWithDual(recordLocations, recordErrors, combination,
					threshold, error, numInterval, numVector, grids, recordId, duals, dualKeys, weights, bins, dimension, rubnerKeys);			
		}
		
		timer += System.nanoTime() - guestTimer;
		
		for (int i = 0; i < worker; i++) {
			workerCombinations[i] = "";
		}
	
		recordString = recordId + " " + FormatUtil.formatDoubleArray(weights);
		String nativeCombination = FormatUtil.formatCombination(gridIds);
		String out = recordString + " " + ntv + " " + nativeCombination;
		outVal.set(out);
		outKey.set(assignment.get(nativeCombination));
		context.write(outKey, outVal);
		
		if (queryType.equals("rank")) {
			// rank results (guest) will be written in the cleanup() method
			return;
		}
		for (String eachGuest : guestCombination) {
			if (shouldDistribute(nativeCombination, eachGuest)) {
				int worker = getWorkerForCombination(eachGuest);
				if (workerCombinations[worker].length() == 0) {
					workerCombinations[worker] = recordString + " " + guest + " " + eachGuest;
				}
				else {
					workerCombinations[worker] += " " + eachGuest;
				}
			}
		}
		
		for (int i = 0; i < worker; i++) {
			if (workerCombinations[i].length() > 0) {
				outKey.set(i);
				outVal.set(workerCombinations[i]);
				context.write(outKey, outVal);
				//System.out.println(outVal.toString() + " distributed to " + outKey.toString());
				
			}
		}

	}
	
	private boolean shouldDistribute(String recordCombination, String combination) {
		double[] recordIds = FormatUtil.toDoubleArray(recordCombination);
		double[] combinationIds = FormatUtil.toDoubleArray(combination);
		double recordSum = HistUtil.sum(recordIds);
		double combinationSum = HistUtil.sum(combinationIds);
		if (recordCombination.equals(combination)) {
			return false;
		}
		if (0 == (int)(recordSum + combinationSum) % 2){
			if (recordCombination.compareTo(combination) > 0) {
				return true;
			}
			else {
				return false;
			}
		}
		else {
			if (recordCombination.compareTo(combination) <= 0) {
				return true;
			}
			else {
				return false;
			}
		}
	}
	
	private int getWorkerForCombination(String combination) {
		return (int) assignment.get(combination).longValue();
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		if (queryType.equals("rank")) {
			globalUpper = Grid.pruneCandidates(rankTmpCandidates, rankCandidates, paraK, globalUpper);
			System.out.println("rank candidate set size: " + rankCandidates.size());
			System.out.println("rank pruned candidates: " + TimerUtil.rankEliminatedCounter);
			for (Candidate candidate : rankCandidates) {
				String recordString = candidate.getRid() + " " + FormatUtil.formatDoubleArray(candidate.getWeights());
				String out = "";
				out = recordString + " " + guest + " " + candidate.getCombination();
				outVal.set(out);
				outKey.set(assignment.get(candidate.getCombination()));
				context.write(outKey, outVal);
			}
		}
		
		else if (queryType.equals("range")) {
			logger.debug("Guest elasped time: " + timer/1000000 + " ms");
			logger.debug("interscetion: " + TimerUtil.intersectionTimer / 1000000 + " ms");
			logger.debug("no intersection " + TimerUtil.noIntersectionTimer / 1000000 + " ms");		
			logger.debug("intersection #: " + TimerUtil.intersectionCounter + " ; no intersection #: " + TimerUtil.noIntersectionCounter);
			logger.debug("computed emdbr #: " + TimerUtil.emdbrCounter);
			System.out.println("Eliminated " + TimerUtil.eliminatedCounter);
			System.out.println("Dual eliminated " + TimerUtil.dualElimination);
		}
	}
}
