package com.iojin.melody.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
//import java.util.Random;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Grid {

	protected double[] lineSouthwestern = new double[3];
	protected double[] lineSoutheastern = new double[3];
	protected double[] lineNortheastern = new double[3];
	protected double[] lineNorthwestern = new double[3];
	protected double[] pointSouthern = new double[2];
	protected double[] pointEastern = new double[2];
	protected double[] pointNorthern = new double[2];
	protected double[] pointWestern = new double[2];
	private double interceptSW = 0.0;
	private double interceptSE = 0.0;
	protected double[] domain = new double[4];
	protected double[] slopes = new double[2];
	protected int sideNum = 0;
	protected double[] t = new double[2];
	private Logger logger;
	private static TreeSet<Candidate> candidates = new TreeSet<Candidate>();
	private static TreeSet<Candidate> rankCandidates = new TreeSet<Candidate>();
	private static double kUpper = Double.MAX_VALUE;

	public Grid(double[] inDomain, double[] inSlope, int inSideNum) {
		initializeGrid(inDomain, inSlope, inSideNum);
		logger = Logger.getLogger(getClass());
		logger.setLevel(Level.DEBUG);
	}

	public long getGridId(double[] record) {
		// record[0] - m, record[1] - b
		// domain[0] - mMin, domain[1] - mMax, domain[2] - bMin, domain[3] -
		// bMax
		// slopes[0] - tMin, slopes[1] - tMax
		int locationSW = getCountOnLine(record, slopes[1],
				Direction.Southwestern);
		int locationSE = getCountOnLine(record, slopes[0],
				Direction.Southeastern);
		if (locationSE == sideNum) {
			locationSE--;
		}
		if (locationSW == sideNum) {
			locationSW--;
		}
		return locationSE * sideNum + locationSW;
	}

	public static List<String> getGuestForKNN(double[] record,
			double[] recordError, String recordCombination,
			double[] combinationError, int numInterval, int numVector,
			Grid[] grids, long recordId, DualBound[] duals,
			HashMap<String, Double[]> dualKeys, double[] hist,
			int numReference, int paraK, List<Double[]> references,
			double[] bins, int dimension, HashMap<String, Double[]> rubnerKeys) {

		Logger logger = Logger.getLogger(Grid.class);
		logger.setLevel(Level.DEBUG);

		int recordErrorLength = 2 * numInterval + 1;
		int errorLength = 2 * (numInterval + 1);
		int combinationLength = numVector * errorLength + 1 + numVector
				+ numReference;
		int numCombination = combinationError.length / combinationLength;

		double[] combination = new double[combinationLength];
		double[] ids = new double[numVector];
		long[] idsLong = new long[ids.length];
		List<String> guest = new ArrayList<String>();
		double[] recordInSpace = new double[2];
		double[] errorInSpace = new double[recordErrorLength];
		double[] gridBound = new double[4];
		double[] gridError = new double[errorLength];
		int counterCombination = 0;

		candidates.clear();
		kUpper = Double.MAX_VALUE;

		System.out.println("Overall " + numCombination + " combinations");
		
		for (int i = 0; i < numCombination; i++) {
			long tmpTimer = System.nanoTime();
			combination = FormatUtil.getNthSubArray(combinationError,
					combinationLength, i, combination);
			
			// logger.debug("Considering combinations string : " +
			// FormatUtil.toString(combination));
			int offset = numVector + 1;
			ids = FormatUtil.getSubArray(combination, 0, numVector - 1, ids);
			for (int j = 0; j < ids.length; j++) {
				idsLong[j] = Math.round(ids[j]);
			}
			String combinationId = FormatUtil.formatCombination(idsLong);
			counterCombination = (int) combination[numVector];

			if (!recordCombination.equals(combinationId)) {

				double maxLower = -Double.MAX_VALUE;
				double minUpper = Double.MAX_VALUE;
				Double[] keys = dualKeys.get(combinationId);

				/*
				 * first try pruning by dual emd
				 */
				for (int k = 0; k < duals.length; k++) {
					double dualLower = duals[k].getCKey(hist) + keys[k];
					maxLower = dualLower > maxLower ? dualLower : maxLower;
					System.out.println("dual " + k + " : " + dualLower);
				}

				/*
				 * then compute normal emd
				 */
				for (int j = 0; j < numVector; j++) {
					recordInSpace = FormatUtil.getNthSubArray(record, 2, j,
							recordInSpace);
					errorInSpace = FormatUtil.getNthSubArray(recordError,
							recordErrorLength, j, errorInSpace);
					long gridId = Math.round(ids[j]);
					gridBound = grids[j].getGrid((int) gridId, gridBound);
					gridError = FormatUtil.getSubArray(combination, offset + j
							* errorLength, offset + (j + 1) * errorLength - 1,
							gridError);
					Direction direction = grids[j].locateRecordToGrid(
							recordInSpace, (int) gridId);

					double emdBr = grids[j].getEmdBr(recordInSpace,
							errorInSpace, gridBound, gridError, direction,
							numInterval);

					maxLower = emdBr > maxLower ? emdBr : maxLower;

					TimerUtil.emdbrCounter++;
				}
				
				/*
				 * then rubner emd
				 */
				double[] rubnerValue = DistanceUtil.getRubnerValue(hist, dimension, bins);
				double rubnerEmd = DistanceUtil.getRubnerBound(rubnerValue, FormatUtil.toDoubleArray(rubnerKeys.get(combinationId)), dimension);
				maxLower = maxLower > rubnerEmd ? maxLower : rubnerEmd;

				double[] gridFlows = FormatUtil.getSubArray(combination,
						numVector * errorLength + 1 + numVector,
						combination.length - 1);
				for (int j = 0; j < numReference; j++) {
					double upper = gridFlows[j]
							+ HistUtil.getFlowBetween(hist, references.get(j),
									bins, dimension);
					minUpper = upper > minUpper ? minUpper : upper;
				}
				
				System.out.println("maxLower: " + maxLower + " ; minUpper: " + minUpper);

				TimerUtil.knnBoundTimer += System.nanoTime() - tmpTimer;
				tmpTimer = System.nanoTime();
				
				if (maxLower < kUpper) {
					if (counterCombination > paraK && minUpper < kUpper) {
						kUpper = minUpper;
					}
					//System.out.println(combinationId + " " + counterCombination + " " + minUpper + " " + maxLower);
					candidates.add(new Candidate(combinationId,
							counterCombination, minUpper, maxLower));
					
				}
				else {
					System.out.println(combinationId + " eliminated since maxLower: " + maxLower + " while kUpper: " + kUpper);
				}
				
				TimerUtil.knnPruneTimer += System.nanoTime() - tmpTimer;
			}
		}

		int counter = 0;
		long tmpTimer = System.nanoTime();
		Iterator<Candidate> it = candidates.iterator();
		System.out.println("after primary pruning, " + candidates.size() + " combinations");
		while (it.hasNext()) {
			Candidate each = it.next();
			counter += each.getCount();
			if (counter > paraK && kUpper > each.getUpper()) {
				kUpper = each.getUpper();
			}
			if (each.getLower() < kUpper) {
				guest.add(each.getCombination());
				System.out.println("Guest Combination " + each.getCombination() + " added, since its lower:" + each.getLower() + " kUppper: " + kUpper);
			}
			else {
				System.out.println(each.getCombination() + " eliminated since its Lower: " + each.getLower() + " while kUpper: " + kUpper);
				
			}
		}
		TimerUtil.knnEliminationTimer += System.nanoTime() - tmpTimer;
		return guest;
	}

	public static TreeSet<Candidate> getGuestForRank(double[] record,
			double[] recordError, String recordCombination, double threshold,
			double[] combinationError, int numInterval, int numVector,
			Grid[] grids, long recordId, DualBound[] duals,
			HashMap<String, Double[]> dualKeys, double[] hist,
			int numReference, int paraK, List<Double[]> references,
			double[] bins, int dimension, double globalUpper, HashMap<String, Double[]> rubnerKeys) {

		Logger logger = Logger.getLogger(Grid.class);
		logger.setLevel(Level.DEBUG);
		// numVector, count, errors
		int recordErrorLength = 2 * numInterval + 1;
		int errorLength = 2 * (numInterval + 1);
		int combinationLength = numVector * errorLength + 1 + numVector
				+ numReference;
		int numCombination = combinationError.length / combinationLength;
		// logger.debug("Number of combinations : " + numCombination);
		double[] combination = new double[combinationLength];
		double[] ids = new double[numVector];
		long[] idsLong = new long[ids.length];
		double[] recordInSpace = new double[2];
		double[] errorInSpace = new double[recordErrorLength];
		double[] gridBound = new double[4];
		double[] gridError = new double[errorLength];
		int counterCombination = 0;

		rankCandidates.clear();
		candidates.clear();
		kUpper = globalUpper;

		for (int i = 0; i < numCombination; i++) {
//			long tmpTimer = System.nanoTime();
			combination = FormatUtil.getNthSubArray(combinationError,
					combinationLength, i, combination);
			// logger.debug("Considering combinations string : " +
			// FormatUtil.toString(combination));
			int offset = numVector + 1;
			ids = FormatUtil.getSubArray(combination, 0, numVector - 1, ids);
			for (int j = 0; j < ids.length; j++) {
				idsLong[j] = Math.round(ids[j]);
			}
			String combinationId = FormatUtil.formatCombination(idsLong);
			counterCombination = (int) combination[numVector];

			if (!recordCombination.equals(combinationId)) {

				double maxLower = -Double.MAX_VALUE;
				double minUpper = Double.MAX_VALUE;
				Double[] keys = dualKeys.get(combinationId);

				for (int k = 0; k < duals.length; k++) {
					double dualLower = duals[k].getCKey(hist) + keys[k];
					maxLower = dualLower > maxLower ? dualLower : maxLower;
				}

				for (int j = 0; j < numVector; j++) {
					recordInSpace = FormatUtil.getNthSubArray(record, 2, j,
							recordInSpace);
					errorInSpace = FormatUtil.getNthSubArray(recordError,
							recordErrorLength, j, errorInSpace);
					long gridId = Math.round(ids[j]);
					gridBound = grids[j].getGrid((int) gridId, gridBound);
					gridError = FormatUtil.getSubArray(combination, offset + j
							* errorLength, offset + (j + 1) * errorLength - 1,
							gridError);
					Direction direction = grids[j].locateRecordToGrid(
							recordInSpace, (int) gridId);

					double emdBr = grids[j].getEmdBr(recordInSpace,
							errorInSpace, gridBound, gridError, direction,
							numInterval);

					maxLower = emdBr > maxLower ? emdBr : maxLower;

//					TimerUtil.emdbrCounter++;
				}
				
				double[] rubnerValue = DistanceUtil.getRubnerValue(hist, dimension, bins);
				double rubnerEmd = DistanceUtil.getRubnerBound(rubnerValue, FormatUtil.toDoubleArray(rubnerKeys.get(combinationId)), dimension);
				maxLower = maxLower > rubnerEmd ? maxLower : rubnerEmd;

//				double[] gridFlows = FormatUtil.getSubArray(combination,
//						numVector * errorLength + 1 + numVector,
//						combination.length - 1);
//				for (int j = 0; j < numReference; j++) {
//				for (int j = 0; j < (5 < numReference ? 5 : numReference); j++) {
//					double upper = gridFlows[j]
//							+ HistUtil.getFlowBetween(hist, references.get(j),
//									bins, dimension);
//					double upper = DistanceUtil.getEmdLTwo(hist, FormatUtil.toDoubleArray(references.get(j)), dimension, bins);
//					minUpper = upper > minUpper ? minUpper : upper;
//				}

//				TimerUtil.rankBoundTimer += System.nanoTime() - tmpTimer;
//				tmpTimer = System.nanoTime();
				
				logger.info("maxLower: " + maxLower + " -- kUpper: " + kUpper);
				if (maxLower < kUpper) {
//					if (counterCombination > paraK && minUpper < kUpper) {
//						kUpper = minUpper;
//					}
					candidates.add(new Candidate(combinationId,
							counterCombination, minUpper, maxLower));
				}
				else {
					TimerUtil.rankEliminatedCounter++;
				}
//				TimerUtil.rankPruneTimer += System.nanoTime() - tmpTimer;
				
				//System.out.println("record " + recordId + " and combination " + combinationId + " lower: " + maxLower + " ; upper: " + minUpper);
			}
		}
//		long tmpTimer = System.nanoTime();
		// final elimination
		pruneCandidates(candidates, rankCandidates, paraK, kUpper);
//		TimerUtil.rankEliminationTimer += System.nanoTime() - tmpTimer;
//		logger.info("candidate size " + rankCandidates.size());
		return rankCandidates;
	}

	public static double pruneCandidates(TreeSet<Candidate> in,
			TreeSet<Candidate> out, int paraK, double upper) {
		int counter = 0;
		Iterator<Candidate> it = in.iterator();
		out.clear();
		
		while (it.hasNext()) {
			Candidate each = it.next();
			counter += each.getCount();
			if (counter > paraK && upper > each.getUpper()) {
				upper = each.getUpper();
			}
			if (each.getLower() < upper) {
				out.add(each);
			}
			else {
				TimerUtil.rankEliminatedCounter++;
			}
		}

		return upper;
	}

	public static List<String> getGuestWithDual(double[] record,
			double[] recordError, String recordCombination, double threshold,
			double[] combinationError, int numInterval, int numVector,
			Grid[] grids, long recordId, DualBound[] duals,
			HashMap<String, Double[]> dualKeys, double[] hist, 
			double[] bins, int dimension, HashMap<String, Double[]> rubnerKeys) {
		Logger logger = Logger.getLogger(Grid.class);
		logger.setLevel(Level.DEBUG);
		// numVector, count, errors
		int recordErrorLength = 2 * numInterval + 1;
		int errorLength = 2 * (numInterval + 1);
		int combinationLength = numVector * errorLength + 1 + numVector;

		int numCombination = combinationError.length / combinationLength;
		// logger.debug("Number of combinations : " + numCombination);
		double[] combination = new double[combinationLength];
		double[] ids = new double[numVector];
		long[] idsLong = new long[ids.length];
		List<String> guest = new ArrayList<String>();
		double[] recordInSpace = new double[2];
		double[] errorInSpace = new double[recordErrorLength];
		double[] gridBound = new double[4];
		double[] gridError = new double[errorLength];

		long counterCombination = 0;

		double[] emdBrOrth = new double[numVector];

		for (int i = 0; i < numCombination; i++) {
			combination = FormatUtil.getNthSubArray(combinationError,
					combinationLength, i, combination);
			// logger.debug("Considering combinations string : " +
			// FormatUtil.toString(combination));
			int offset = numVector + 1;
			ids = FormatUtil.getSubArray(combination, 0, numVector - 1, ids);
			for (int j = 0; j < ids.length; j++) {
				idsLong[j] = Math.round(ids[j]);
			}
			String combinationId = FormatUtil.formatCombination(idsLong);
			boolean guestCombination = true;

			counterCombination = (long) combination[numVector];

			if (!recordCombination.equals(combinationId)) {

				Double[] keys = dualKeys.get(combinationId);

				/*
				 * first try pruning by dual emd
				 */
				for (int k = 0; k < duals.length; k++) {
					double[] range = duals[k].getRange(hist, threshold);
					// if min > max or max < min
					if (keys[k] > range[1] || keys[duals.length + k] < range[0]) {
						guestCombination = false;
						break;
					}
				}
				if (!guestCombination) {
					TimerUtil.dualElimination += counterCombination;
					TimerUtil.eliminatedCounter += counterCombination;
					continue;
				}

				/*
				 * then compute normal emd
				 */
				for (int j = 0; j < numVector; j++) {
					recordInSpace = FormatUtil.getNthSubArray(record, 2, j,
							recordInSpace);
					errorInSpace = FormatUtil.getNthSubArray(recordError,
							recordErrorLength, j, errorInSpace);
					long gridId = Math.round(ids[j]);
					gridBound = grids[j].getGrid((int) gridId, gridBound);
					gridError = FormatUtil.getSubArray(combination, offset + j
							* errorLength, offset + (j + 1) * errorLength - 1,
							gridError);
					Direction direction = grids[j].locateRecordToGrid(
							recordInSpace, (int) gridId);

					double emdBr = grids[j].getEmdBr(recordInSpace,
							errorInSpace, gridBound, gridError, direction,
							numInterval);
					TimerUtil.emdbrCounter++;
					emdBrOrth[j] = emdBr;
					if (emdBr > threshold) {
						TimerUtil.eliminatedCounter += counterCombination;
						guestCombination = false;
						break;
					}
				}
				
				double[] rubnerValue = DistanceUtil.getRubnerValue(hist, dimension, bins);
				double rubnerEmd = DistanceUtil.getRubnerBound(rubnerValue, FormatUtil.toDoubleArray(rubnerKeys.get(combinationId)), dimension);
				if (rubnerEmd > threshold) {
					TimerUtil.eliminatedCounter += counterCombination;
					guestCombination = false;
//					break;					
				}
			}
			if (guestCombination) {
				guest.add(combinationId);
			}
		}
		return guest;
	}

	public static List<String> getGuestForCombination(
			double[] record,
			double[] recordError, // min max min max ... min max full
			String recordCombination, double threshold,
			double[] combinationError, // numVector count (min max min max ...
										// min max minFull maxFull)_1 ....
			int numInterval, int numVector, Grid[] grids, long recordId) {
		Logger logger = Logger.getLogger(Grid.class);
		logger.setLevel(Level.DEBUG);
		// numVector, count, errors
		int recordErrorLength = 2 * numInterval + 1;
		int errorLength = 2 * (numInterval + 1);
		int combinationLength = numVector * errorLength + 1 + numVector;
		int numCombination = combinationError.length / combinationLength;
		// logger.debug("Number of combinations : " + numCombination);
		double[] combination = new double[combinationLength];
		double[] ids = new double[numVector];
		long[] idsLong = new long[ids.length];
		List<String> guest = new ArrayList<String>();
		double[] recordInSpace = new double[2];
		double[] errorInSpace = new double[recordErrorLength];
		double[] gridBound = new double[4];
		double[] gridError = new double[errorLength];

		long counterCombination = 0;

		double[] emdBrOrth = new double[numVector];

		for (int i = 0; i < numCombination; i++) {
			combination = FormatUtil.getNthSubArray(combinationError,
					combinationLength, i, combination);
			// logger.debug("Considering combinations string : " +
			// FormatUtil.toString(combination));
			int offset = numVector + 1;
			ids = FormatUtil.getSubArray(combination, 0, numVector - 1, ids);
			for (int j = 0; j < ids.length; j++) {
				idsLong[j] = Math.round(ids[j]);
			}
			String combinationId = FormatUtil.formatCombination(idsLong);
			boolean guestCombination = true;

			counterCombination = (long) combination[numVector];

			if (!recordCombination.equals(combinationId)) {

				for (int j = 0; j < numVector; j++) {
					recordInSpace = FormatUtil.getNthSubArray(record, 2, j,
							recordInSpace);
					errorInSpace = FormatUtil.getNthSubArray(recordError,
							recordErrorLength, j, errorInSpace);
					long gridId = Math.round(ids[j]);
					gridBound = grids[j].getGrid((int) gridId, gridBound);
					gridError = FormatUtil.getSubArray(combination, offset + j
							* errorLength, offset + (j + 1) * errorLength - 1,
							gridError);
					Direction direction = grids[j].locateRecordToGrid(
							recordInSpace, (int) gridId);

					double emdBr = grids[j].getEmdBr(recordInSpace,
							errorInSpace, gridBound, gridError, direction,
							numInterval);
					TimerUtil.emdbrCounter++;
					emdBrOrth[j] = emdBr;
					if (emdBr > threshold) {
						TimerUtil.eliminatedCounter += counterCombination;
						guestCombination = false;
						if (recordId == 16775
								&& combinationId.equals("3 0 3 3 1 3 3")) {
							logger.debug("Record " + recordId + " in space "
									+ j + " (" + direction + "), combination "
									+ combinationId + " is eliminated by "
									+ emdBr);
							logger.debug("record : "
									+ FormatUtil.toString(recordInSpace));
							logger.debug("record error: "
									+ FormatUtil.toString(errorInSpace));
							logger.debug("grid : "
									+ FormatUtil.toString(gridBound));
							logger.debug("grid error: "
									+ FormatUtil.toString(gridError));
						}
						break;
					}
				}
			}
			if (guestCombination) {
				guest.add(combinationId);
			}
		}
		return guest;
	}

	public Direction locateRecordToGrid(double[] record, double[] gridBound) {
		double[] pointInGrid = new double[2];
		pointInGrid[0] = (gridBound[0] + gridBound[2]) / 2;
		pointInGrid[1] = (gridBound[1] + gridBound[3]) / 2;
		return locateRecordToGrid(record, (int) getGridId(pointInGrid));
	}

	public Direction locateRecordToGrid(double[] record, int gridId) {
		int locationSW = gridId % sideNum;
		int locationSE = gridId / sideNum;
		int recordLocationSW = getCountOnLine(record, slopes[1],
				Direction.Southwestern);
		int recordLocationSE = getCountOnLine(record, slopes[0],
				Direction.Southeastern);
		int sw = recordLocationSW - locationSW;
		int se = recordLocationSE - locationSE;
		Direction direction = Direction.Inner;
		if (sw > 0 && se > 0)
			direction = Direction.Northern;
		else if (sw > 0 && 0 == se)
			direction = Direction.Northwestern;
		else if (sw > 0 && se < 0)
			direction = Direction.Western;
		else if (0 == sw && se > 0)
			direction = Direction.Northeastern;
		else if (0 == sw && se < 0)
			direction = Direction.Southwestern;
		else if (sw < 0 && se > 0)
			direction = Direction.Eastern;
		else if (sw < 0 && 0 == se)
			direction = Direction.Southeastern;
		else if (sw < 0 && se < 0)
			direction = Direction.Southern;

		// special case, when a record is the min/max records that define the
		// grid space
		double epsilon = 0.000001;
		// logger.debug("record " + FormatUtil.toString(record)
		// + " : domain " + FormatUtil.toString(domain));
		if (Math.abs(record[0] - domain[0]) <= epsilon
				&& Math.abs(record[1] - domain[2]) <= epsilon
				|| Math.abs(record[0] - domain[1]) <= epsilon
				&& Math.abs(record[1] - domain[3]) <= epsilon) {
			direction = Direction.Inner;
		}

		return direction;
	}

	public static HashMap<Long, Long> assignGrid(double[] count, int worker) {
		HashMap<Long, Long> assignment = new HashMap<Long, Long>();
		TreeMap<Long, Long> countGrid = new TreeMap<Long, Long>();
		/*
		 * count gridId count gridId count ...
		 */
		for (int i = 0; i < count.length / 2; i++) {
			countGrid.put(Math.round(count[2 * i + 1]),
					Math.round(count[2 * i]));
		}
		HashMap<Long, Long> workerLoads = new HashMap<Long, Long>();
		for (int i = 0; i < worker; i++) {
			workerLoads.put(Long.valueOf(i), Long.valueOf(0));
		}
		for (Map.Entry<Long, Long> entry : countGrid.entrySet()) {
			long leastWorker = findWorkerWithLeastLoads(workerLoads);
			// using native * native as the indicator
			workerLoads.put(
					leastWorker,
					(long) Math.pow(workerLoads.get(leastWorker), 2)
							+ entry.getValue());
			assignment.put(entry.getKey(), leastWorker);
		}
		return assignment;
	}

	public static HashMap<String, Long> assignGrid(HashMap<String, Long> count,
			int worker) {
		HashMap<String, Long> assignment = new HashMap<String, Long>();
		TreeMap<Long, List<String>> countGrid = new TreeMap<Long, List<String>>();
		long sum = 0;
		for (Map.Entry<String, Long> entry : count.entrySet()) {
			if (!countGrid.containsKey(entry.getValue())) {
				countGrid.put(entry.getValue(), new ArrayList<String>());
			}
			countGrid.get(entry.getValue()).add(entry.getKey());
			sum += entry.getValue();
		}
		HashMap<Long, Long> workloads = new HashMap<Long, Long>();
		for (int i = 0; i < worker; i++) {
			workloads.put(Long.valueOf(i), Long.valueOf(0));
		}
		for (Long key : countGrid.descendingKeySet()) {
			for (String combination : countGrid.get(key)) {
				long leastWorker = findWorkerWithLeastLoads(workloads);
				long additionalLoads = (long) Math.pow(key, 2) + key
						* (sum - key);
				workloads.put(leastWorker,
						additionalLoads + workloads.get(leastWorker));
				assignment.put(combination, leastWorker);
			}
		}
		// Logger logger = Logger.getLogger(Grid.class);
		// logger.setLevel(Level.DEBUG);
		// logger.debug("Workloads: " + FormatUtil.toStringLong(workloads));
		return assignment;
	}
	
	public static HashMap<String, Integer> assignGridInt(HashMap<String, Integer> count,
			int worker) {
		HashMap<String, Integer> assignment = new HashMap<String, Integer>();
		TreeMap<Integer, List<String>> countGrid = new TreeMap<Integer, List<String>>();
		long sum = 0;
		for (Map.Entry<String, Integer> entry : count.entrySet()) {
			if (!countGrid.containsKey(entry.getValue())) {
				countGrid.put(entry.getValue(), new ArrayList<String>());
			}
			countGrid.get(entry.getValue()).add(entry.getKey());
			sum += entry.getValue();
		}
		HashMap<Long, Long> workloads = new HashMap<Long, Long>();
		for (int i = 0; i < worker; i++) {
			workloads.put(Long.valueOf(i), Long.valueOf(0));
		}
		for (Integer key : countGrid.descendingKeySet()) {
			for (String combination : countGrid.get(key)) {
				long leastWorker = findWorkerWithLeastLoads(workloads);
				long additionalLoads = (long) Math.pow(key, 2) + key
						* (sum - key);
				workloads.put(leastWorker,
						additionalLoads + workloads.get(leastWorker));
				assignment.put(combination, (int) leastWorker);
			}
		}
		// Logger logger = Logger.getLogger(Grid.class);
		// logger.setLevel(Level.DEBUG);
		// logger.debug("Workloads: " + FormatUtil.toStringLong(workloads));
		return assignment;
	}

	/*
	 * get the projections of a point on line SW and line SE
	 */
	public double[] getProjectionsInGrid(double[] point) {
		double[] projections = new double[4]; // sw - 2, se - 2
		double[] lineProjection = getLine(point, slopes[1]);
		double[] eachProjection = getIntersection(lineProjection,
				lineSouthwestern);
		projections[0] = eachProjection[0];
		projections[1] = eachProjection[1];
		lineProjection = getLine(point, slopes[0]);
		eachProjection = getIntersection(lineProjection, lineSoutheastern);
		projections[2] = eachProjection[0];
		projections[3] = eachProjection[1];
		return projections;
	}

	/*
	 * get the distances between projections and the southern point on line SW
	 * and line SE, which are used to compute the quantiles
	 */
	public double[] getProjectionDistanceInGrid(double[] point) {
		double[] projections = getProjectionsInGrid(point);
		double[] distance = new double[2];
		distance[0] = getDistance(FormatUtil.getSubArray(projections, 0, 1),
				pointSouthern);
		distance[1] = getDistance(FormatUtil.getSubArray(projections, 2, 3),
				pointSouthern);
		return distance;
	}

	private void initializeGrid(double[] inDomain, double[] inSlopes,
			int inSideNum) {

		for (int i = 0; i < inDomain.length; i++) {
			domain[i] = inDomain[i];
		}
		
		slopes[0] = inSlopes[0];
		slopes[1] = inSlopes[1];
		t[0] = -slopes[1];
		t[1] = -slopes[0];
		
 		sideNum = inSideNum;
		lineSouthwestern[0] = slopes[0];
		lineSouthwestern[1] = domain[0];
		lineSouthwestern[2] = domain[2];
		lineSoutheastern[0] = slopes[1];
		lineSoutheastern[1] = domain[1];
		lineSoutheastern[2] = domain[2];
		lineNortheastern[0] = slopes[0];
		lineNortheastern[1] = domain[1];
		lineNortheastern[2] = domain[3];
		lineNorthwestern[0] = slopes[1];
		lineNorthwestern[1] = domain[0];
		lineNorthwestern[2] = domain[3];
		pointSouthern = getIntersection(lineSouthwestern, lineSoutheastern);
		pointEastern = getIntersection(lineSoutheastern, lineNortheastern);
		pointNorthern = getIntersection(lineNortheastern, lineNorthwestern);
		pointWestern = getIntersection(lineSouthwestern, lineNorthwestern);

		interceptSW = getDistance(pointSouthern, pointWestern) / sideNum;
		interceptSE = getDistance(pointEastern, pointSouthern) / sideNum;
	}

	private static Long findWorkerWithLeastLoads(HashMap<Long, Long> workloads) {
		long leastKey = Long.MAX_VALUE;
		long leastLoads = Long.MAX_VALUE;
		for (Map.Entry<Long, Long> entry : workloads.entrySet()) {
			if (entry.getValue() < leastLoads) {
				leastKey = entry.getKey();
				leastLoads = entry.getValue();
			}
		}
		return leastKey;
	}

	protected double[] getIntersection(double[] lineA, double[] lineB) {
		// line[0] - slope, line[1] - m, line[2] b
		if (lineA[0] != lineB[0]) {
			double[] intersection = new double[2];
			intersection[0] = (lineB[2] - lineA[2] + lineA[0] * lineA[1] - lineB[0]
					* lineB[1])
					/ (lineA[0] - lineB[0]);
			intersection[1] = lineB[0] * (intersection[0] - lineB[1])
					+ lineB[2];
			return intersection;
		} else
			return null;
	}

	protected static double getDistance(double[] pointA, double[] pointB) {
		return Math.sqrt(Math.pow(pointA[0] - pointB[0], 2)
				+ Math.pow(pointA[1] - pointB[1], 2));
	}

	protected int getCountOnLine(double[] record, double lineSlope,
			Direction direction) {
//		int count = 0;
//		double[] line = new double[3];
		// double intercept = 0.0;
		double[] projections = getProjectionDistanceInGrid(record);
		double offset = 0.0;
		switch (direction) {
		case Southwestern:
//			line = lineSouthwestern;
			// intercept = tMinIntercept;
			offset = projections[0];
			break;
		case Southeastern:
//			line = lineSoutheastern;
			// intercept = tMaxIntercept;
			offset = projections[1];
			break;
		default:
			return -1;
		}
//		double[] recordLine = getLine(record, lineSlope);
//		double[] intersection = getIntersection(line, recordLine);
//		double offset = getDistance(intersection, pointSouthern);
		// logger.debug("Intersection: " + FormatUtil.toString(intersection) +
		// "; offset " + offset);
		// logger.debug("Before rounding: " + offset / intercept);
		// location = (int) Math.round(Math.floor(offset / intercept));
		int count = countOffset(offset, direction);
		// logger.debug("After rounding: " + location);
		return count;
	}

	protected int countOffset(double offset, Direction direction) {
		double intercept;
		switch (direction) {
		case Southwestern:
			intercept = interceptSW;
			break;
		case Southeastern:
			intercept = interceptSE;
			break;
		default:
			logger.debug("Somehow here " + offset);
			return -1;
		}
		return (int) Math.round(Math.floor(offset / intercept));
	}

	public double[] getGridBound(int gridId) {

		int locationSW = gridId % sideNum;
		int locationSE = gridId / sideNum;
		double[] lower = getIntersectionByCount(locationSW, locationSE);
		locationSW++;
		locationSE++;
		double[] upper = getIntersectionByCount(locationSW, locationSE);
		double[] bounding = new double[4];
		bounding[0] = lower[0];
		bounding[1] = lower[1];
		bounding[2] = upper[0];
		bounding[3] = upper[1];
		// logger.debug("Grid # " + gridId + " : " +
		// FormatUtil.toString(bounding));
		return bounding;
	}

	public double[] getGrid(int gridId, double[] bounding) {

		int locationSW = gridId % sideNum;
		int locationSE = gridId / sideNum;
		if (locationSW < 0 || locationSE < 0) {
			logger.debug("location SW: " + locationSW + "; SE: " + locationSE);
		}
		double[] lower = getIntersectionByCount(locationSW, locationSE);
		locationSW++;
		locationSE++;
		double[] upper = getIntersectionByCount(locationSW, locationSE);
		bounding[0] = lower[0];
		bounding[1] = lower[1];
		bounding[2] = upper[0];
		bounding[3] = upper[1];
		// logger.debug("Grid # " + gridId + " : " +
		// FormatUtil.toString(bounding));
		return bounding;
	}

	protected double[] getIntersectionByCount(int countSW, int countSE) {
		double[] pointSW = new double[2];
		double[] pointSE = new double[2];
		pointSW[0] = pointSouthern[0] - getX(interceptSW * countSW, slopes[0]);
		pointSW[1] = pointSouthern[1] + getY(interceptSW * countSW, slopes[0]);
		double[] lineSE = getLine(pointSW, slopes[1]);
		pointSE[0] = pointSouthern[0] + getX(interceptSE * countSE, slopes[1]);
		pointSE[1] = pointSouthern[1] + getY(interceptSE * countSE, slopes[1]);
		double[] lineSW = getLine(pointSE, slopes[0]);
		double[] intersect = getIntersection(lineSE, lineSW);
		return intersect;
	}

	protected double getY(double length, double slope) {
		return getX(length, slope) * Math.abs(slope);
	}

	protected double getX(double length, double slope) {
		return length / Math.sqrt(1 + Math.pow(slope, 2));
	}

	/*
	 * This method actually does not relate to the exact grid division, i.e., it
	 * stays the same for grids in different spaces and grids with different
	 * sizes
	 */
	public double getEmdBr(double[] record, double[] recordError,
			double[] gridBound, double[] gridError, Direction direction,
			int numInterval) {

		// DebugUtil.areaNegative = false;

		double dist = 0.0;
		double[] normalRecord = getNormal(record);
		// double[] normalUpper = getNormal(FormatUtil.getSubArray(gridBound, 0,
		// 1));
		// double[] normalLower = getNormal(FormatUtil.getSubArray(gridBound, 2,
		// 3));
		double[] normalUpper = getNormal(FormatUtil
				.getSubArray(gridBound, 2, 3));
		double[] normalLower = getNormal(FormatUtil
				.getSubArray(gridBound, 0, 1));
		// the standard deviation cannot be negative
		if (normalUpper[1] < 0 || normalLower[1] < 0 || normalRecord[1] < 0) {
			return 0.0;
		}
		double[] dottedLine = new double[3];
		double[] normalIs = new double[2];
		double[] lineNE = getLine(FormatUtil.getSubArray(gridBound, 2, 3),
				slopes[0]);
		double[] lineNW = getLine(FormatUtil.getSubArray(gridBound, 2, 3),
				slopes[1]);
		double[] lineSE = getLine(FormatUtil.getSubArray(gridBound, 0, 1),
				slopes[1]);
		double[] lineSW = getLine(FormatUtil.getSubArray(gridBound, 0, 1),
				slopes[0]);

		switch (direction) {
		case Northern: // complete dominance
			dist = getEmdBrNormal(normalRecord, normalUpper)
					+ getEmdBrError(normalRecord, recordError, normalUpper,
							gridError, numInterval, false);
			break;
		case Southern: // complete dominance
			dist = getEmdBrNormal(normalRecord, normalLower)
					+ getEmdBrError(normalLower, gridError, normalRecord,
							recordError, numInterval, false);
			break;
		case Northeastern: // partial dominance
			dottedLine = getLine(record, slopes[1]);
			normalIs = getNormal(getIntersection(dottedLine, lineNE));
			break;
		case Northwestern: // partial dominance
			dottedLine = getLine(record, slopes[0]);
			normalIs = getNormal(getIntersection(dottedLine, lineNW));
			break;
		case Southeastern: // partial dominance
			dottedLine = getLine(record, slopes[0]);
			normalIs = getNormal(getIntersection(dottedLine, lineSE));
			break;
		case Southwestern: // partial dominance
			dottedLine = getLine(record, slopes[1]);
			normalIs = getNormal(getIntersection(dottedLine, lineSW));
			break;
		case Eastern: // no dominance
			normalIs = getNormal(getIntersection(lineNE, lineSE));
			break;
		case Western: // no dominance
			normalIs = getNormal(getIntersection(lineNW, lineSW));
			break;
		default:
			break;
		}
		if (normalIs[1] < 0) {
			return 0.0;
		}
		switch (direction) {
		case Northeastern: // partial dominance
		case Northwestern: // partial dominance
//			dist = (1 / 2)
//					* (getEmdBrNormal(normalUpper, normalRecord)
//							+ getEmdBrNormal(normalIs, normalRecord) - getEmdBrNormal(
//								normalIs, normalUpper));
			double temp1 = getEmdBrNormal(normalUpper, normalRecord);
			double temp2 = getEmdBrNormal(normalIs, normalRecord);
			double temp3 = getEmdBrNormal(normalIs, normalUpper);
			
			dist = (1.0/2.0) * (temp1 + temp2 - temp3);
			
			dist += getEmdBrError(normalRecord, recordError, normalUpper,
					gridError, numInterval, true);
			break;
		case Southeastern: // partial dominance
		case Southwestern: // partial dominance
			dist = (1.0/2.0) 
					* (getEmdBrNormal(normalLower, normalRecord)
							+ getEmdBrNormal(normalIs, normalRecord) - getEmdBrNormal(
								normalIs, normalLower));
//			dist = (1.0/2.0) 
//					* (getEmdBrNormal(normalIs, normalRecord)
//							+ getEmdBrNormal(normalIs, normalLower) - getEmdBrNormal(
//								normalRecord, normalLower));
			dist += getEmdBrError(normalLower, gridError, normalRecord,
					recordError, numInterval, true);
			break;
		case Eastern: // no dominance
		case Western: // no dominance
			dist = (1.0/2.0) 
					* (getEmdBrNormal(normalLower, normalRecord)
							+ getEmdBrNormal(normalIs, normalRecord) - getEmdBrNormal(
								normalIs, normalLower));
			dist += getEmdBrError(normalLower, gridError, normalRecord,
					recordError, numInterval, true);
//			dist += getEmdBrError(normalRecord, recordError, normalLower, gridError, numInterval, true);
			double min = dist;
			dist = (1.0/2.0) 
					* (getEmdBrNormal(normalUpper, normalRecord)
							+ getEmdBrNormal(normalIs, normalRecord) - getEmdBrNormal(
								normalIs, normalUpper));
			dist += getEmdBrError(normalRecord, recordError, normalUpper,
					gridError, numInterval, true);
			dist = dist > min ? min : dist;
			break;
		case Inner:
			dist = 0.0;
		default:
			break;
		}

		return dist > 0 ? dist : 0;
	}

	private double getEmdBrNormal(double[] record, double[] grid) {
		double dist = 0.0;
		double intersection = HistUtil.getNormalCDFIntersection(record, grid);
//		double min = (-1) * t[1];
//		double max = (-1) * t[0];
		double min = t[0];
		double max = t[1];

		if ((min - intersection) > 0 || (intersection - max) > 0 
				|| Math.abs(min - intersection) <= 0.0000001
				|| Math.abs(intersection - max) <= 0.0000001) {
			dist = HistUtil.getCDFDifferenceBetweenNormals(record, grid, min,
					max);
		}

		else {
			dist = HistUtil.getCDFDifferenceBetweenNormals(record, grid, min,
					intersection)
					+ HistUtil.getCDFDifferenceBetweenNormals(record, grid,
							intersection, max);
		}

		return dist;
	}

	// Notice that for record, the error is only of length 2 * interval + 1, min
	// max min max ... min max full
	// while for a gird, the error is of length 2 * interval + 2, min max min
	// max ... min max minFull maxFull
	private double getEmdBrError(double[] dominated, double[] dominatedError,
			double[] dominator, double[] dominatorError, int numInterval,
			boolean partial) {
		double error = 0.0;
		double intersection = HistUtil.getNormalCDFIntersection(dominated,
				dominator);
		if (!partial) {
			if (intersection <= t[0] || intersection >= t[1]) {
				// - full dominator + full dominated
				// this can be tricky, as record has 2*interval + 1, 2 *
				// interval is the full error
				// the grid has 2 * interval + 2, 2 * interval is the min full
				// error, and 2 * interval + 1 is the max full error
				// no matter who dominates, the last error should be subtracted
				// while the 2 * interval should be plus
				error = -dominatorError[dominatorError.length - 1]
						+ dominatedError[2 * numInterval];
			} else {
				// logger.debug("this should be impossible as it is granted by the dominance lines");
				// intersection is in intersectionInterval
				// - intersection dominator + intersection dominated
				// - max so the iterator should be intersection * 2 + 1 for
				// dominator
				// + min so the iterator should be intersection * 2 for
				// dominated
				int intersectionInterval = (int) Math.round(Math
						.floor((intersection - t[0])
								/ ((t[1] - t[0]) / numInterval)));
				error = -dominatorError[intersectionInterval * 2 + 1]
						+ dominatedError[2 * intersectionInterval];
			}
		} else {
			// partial error only consider the maximum in maxError for dominator
			// and the minimum in minError for dominated
			double maxError = -Double.MAX_VALUE;
			double minError = Double.MAX_VALUE;
			for (int i = 0; i < numInterval; i++) {
				maxError = dominatorError[2 * i + 1] > maxError ? dominatorError[2 * i + 1]
						: maxError;
				minError = dominatedError[2 * i] < minError ? dominatedError[2 * i]
						: minError;
			}
			error = -maxError + minError;
		}

		return error;
	}

	private static double[] getNormal(double[] location) {
		double[] normal = new double[2];
		normal[1] = 1 / location[0];
		normal[0] = location[1] * normal[1] * (-1);
		// if (normal[1] < 0) {
		// System.out.println("invalid normal : " +
		// FormatUtil.toString(normal));
		// }
		return normal;
	}

	protected static double[] getLine(double[] point, double slope) {
		double[] line = new double[3];
		line[0] = slope;
		line[1] = point[0];
		line[2] = point[1];
		return line;
	}

	@Override
	public String toString() {
		String display = "Grid: " + sideNum + "x" + sideNum + "\n";
		display += "southern: " + pointToString(pointSouthern) + "\n";
		display += "northern: " + pointToString(pointNorthern) + "\n";
		display += "western: " + pointToString(pointWestern) + "\n";
		display += "eastern: " + pointToString(pointEastern) + "\n";
		display += "negative intercept: " + interceptSW + "\n";
		display += "positive intercept: " + interceptSE + "\n";
		return display;
	}

	private static String pointToString(double[] point) {
		return "(" + point[0] + ", " + point[1] + ")";
	}

	public static void main(String[] args) {

		double[] record = { -0.19918728452240975, 0.4586114103296915 };
		double[] grid = { -0.499878012442731, 0.0110441240623708 };
		System.out.println(HistUtil.getCDFDifferenceBetweenNormals(record,
				grid, -0.8, 0.8));
		System.exit(0);
	}
}
