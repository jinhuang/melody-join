package com.iojin.melody.utils;

//import java.text.DecimalFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math3.geometry.euclidean.threed.Plane;
import org.apache.commons.math3.geometry.euclidean.threed.Vector3D;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
/**
 * The Histogram Utility class
 * @author soone
 * 
 */
public class HistUtil {
	
	private static final double epsilon = 0.00000001;
	private static final Random random = new Random();
	private static Logger logger = Logger.getLogger(HistUtil.class);
	private static NormalDistributionImpl stdNormal = new NormalDistributionImpl(0, 1);
	
	private static TreeSet<Bin> weightsA = new TreeSet<Bin>();
	private static TreeSet<Bin> weightsB = new TreeSet<Bin>();
	
	private static HashMap<Integer, TreeSet<BinDistance>> groundDistance = null;
	
//	private static Percentile statistics = new Percentile();
	
	public static double getCDFDifferenceBetweenNormals(double[] normalA, double[] normalB, double start, double end) {
		if (Math.abs(normalA[1]) == 0) {
			normalA[1] = epsilon;
		}
		if (Math.abs(normalB[1]) == 0) {
			normalB[1] = epsilon;
		}
		
		if (normalA[1] < 0) {
			System.out.println(FormatUtil.toString((normalA)));
		}
		if (normalB[1] < 0) {
			System.out.println(FormatUtil.toString((normalB)));
		}
		
		NormalDistributionImpl normalDistA = new NormalDistributionImpl(normalA[0], normalA[1]);
		NormalDistributionImpl normalDistB = new NormalDistributionImpl(normalB[0], normalB[1]);
		
		double dist = 0.0;

			double areaA = getNormalCDFAreaBetween(normalDistA, start, end);
			double areaB = getNormalCDFAreaBetween(normalDistB, start, end);
			dist =  Math.abs(areaA - areaB);

		return dist;
	}
	
	public static double getNormalEmd(double[] histA, double[] histB, double[] bins, int numIntervals) {
		double dist = 0.0;
		NormalDistributionImpl normalA = getNormal(histA, bins);
		NormalDistributionImpl normalB = getNormal(histB, bins);
		TreeMap<Double, Double> cdfA = getDiscreteCDFNormalized(histA, bins);
		TreeMap<Double, Double> cdfB = getDiscreteCDFNormalized(histB, bins);
//		TreeMap<Double, Double> cdfA = getDiscreteCDF(histA, bins);
//		TreeMap<Double, Double> cdfB = getDiscreteCDF(histB, bins);		
		double intersect = getNormalCDFIntersection(normalA, normalB);
		List<Double> minMaxErrA = getMinMaxError(normalA, cdfA, numIntervals);
		List<Double> minMaxErrB = getMinMaxError(normalB, cdfB, numIntervals);
		
		double tMin = cdfA.firstKey();
		double tMax = cdfA.lastKey();
		double fullErrA = getFullError(normalA, cdfA, tMin, tMax);
		double fullErrB = getFullError(normalB, cdfB, tMin, tMax);

		//System.out.println("intersection: " + intersect + " tMin: " + tMin + " tMax: " + tMax);
		if (intersect > tMin && intersect < tMax) {
			dist = Math.abs(getNormalCDFAreaBetween(normalA, tMin, intersect) - getNormalCDFAreaBetween(normalB, tMin, intersect))
					+ Math.abs(getNormalCDFAreaBetween(normalA, intersect, tMax) - getNormalCDFAreaBetween(normalB, intersect, tMax));
			double dominance  = getNormalCDFAreaBetween(normalA, tMin, intersect) - getNormalCDFAreaBetween(normalB, tMin, intersect);
			// A dominates B
			if (dominance <= 0 ) {
				dist = dist - lookupError(minMaxErrA, false, tMin, tMax, intersect) + lookupError(minMaxErrB, true, tMin, tMax, intersect);
			}
			else if (dominance > 0 ) {
				dist = dist + lookupError(minMaxErrA, true, tMin, tMax, intersect) - lookupError(minMaxErrB, false, tMin, tMax, intersect);
			}
		}
		else {
//			System.out.println("area A: " + getNormalCDFAreaBetween(normalA, tMin, tMax));
//			System.out.println("area B: " + getNormalCDFAreaBetween(normalB, tMin, tMax));
//			System.out.println("Discrete CDF difference: " + (getDiscreteCDFAreaBetween(cdfA, tMin, tMax) - getDiscreteCDFAreaBetween(cdfB, tMin, tMax)));
//			System.out.println("Discrete A: " + getDiscreteCDFAreaBetween(cdfA, tMin, tMax));
//			System.out.println("Discrete B: " + getDiscreteCDFAreaBetween(cdfB, tMin, tMax));
			dist = getNormalCDFAreaBetween(normalA, tMin, tMax) - getNormalCDFAreaBetween(normalB, tMin, tMax);
			if (dist < 0) {
				dist = Math.abs(dist) - fullErrA + fullErrB;
			}
			else if (dist > 0) {
				dist = Math.abs(dist) + fullErrA - fullErrB;
			}
		}
		if (dist < 0) {
			dist = 0;
		}
		return dist;
	}
	
	public static double getProjectEmd(double[] histA, double[] histB, double[] bins) {
//		TreeMap<Double, Double> cdfA = getDiscreteCDFNormalized(histA, bins);
//		TreeMap<Double, Double> cdfB = getDiscreteCDFNormalized(histB, bins);
		TreeMap<Double, Double> cdfA = getDiscreteCDF(histA, bins);
		TreeMap<Double, Double> cdfB = getDiscreteCDF(histB, bins);		
		double area = 0.0;
		for (Double key : cdfA.keySet()) {
			if (key != cdfA.lastKey()) {
				area += Math.abs((cdfA.get(key) - cdfB.get(key))* (cdfA.higherKey(key) - key));
			}
		}
		return area;
	}
	
	public static double getNormalEmd(double[] histA, double[] histB, double[] bins, int numIntervals, int dimension) {
		double emdSum = 0.0;
		double[] vector = new double[dimension];
		double[] emds = new double[dimension];
		int projectDimension = 0;
		if (dimension == 3) {
			vector = randomBin(vector);		
			double[] orv = new double[dimension];
			double[] thirdv = new double[dimension];
			emds[0] = getNormalEmd(histA, histB, projectBins(bins, dimension, unitArray(vector)), numIntervals);
			orv = get3DOrthogonal(vector);
			emds[1] = getNormalEmd(histA, histB, projectBins(bins, dimension, unitArray(orv)), numIntervals);			
			thirdv = get3DOrthogonal(vector, orv);
			emds[2] = getNormalEmd(histA, histB, projectBins(bins, dimension, unitArray(thirdv)), numIntervals);	
		}
		else {
			for (int i = 0; i < dimension; i++) {
				if(i > 0) {
					vector[i-1] = 0;
				}
				vector[i] = 1;
				emds[i] = getNormalEmd(histA, histB, projectBins(bins, dimension, vector), numIntervals);
			}
		}
		double maxEmd = emds[0];
		
		// use the 
		for (int i = 0; i < dimension; i++) {
			if (emds[i] > 0) {
				emdSum += emds[i];
				projectDimension ++;
			}
			maxEmd = maxEmd > emds[i] ? maxEmd : emds[i];
		}
		return projectDimension == 0 ? maxEmd : (1/Math.sqrt(projectDimension))*emdSum;
	}
	
	public static double getProjectEmd(double[] histA, double[] histB, double[] bins, int dimension) {
		double emdSum = 0.0;
		double[] vector = new double[dimension];
		double[] emds = new double[dimension];
		if (dimension == 3) {
			vector = randomBin(vector);		
			double[] orv = new double[dimension];
			double[] thirdv = new double[dimension];
			emds[0] = getProjectEmd(histA, histB, projectBins(bins, dimension, unitArray(vector)));
			orv = get3DOrthogonal(vector);
			emds[1] = getProjectEmd(histA, histB, projectBins(bins, dimension, unitArray(orv)));		
			thirdv = get3DOrthogonal(vector, orv);
			emds[2] = getProjectEmd(histA, histB, projectBins(bins, dimension, unitArray(thirdv)));		
		}
		else {
			for (int i = 0; i < dimension; i++) {
				if(i > 0) {
					vector[i-1] = 0;
				}
				vector[i] = 1;
				emds[i] = getProjectEmd(histA, histB, projectBins(bins, dimension, vector));
			}
		}
		for (int i = 0; i < dimension; i++) {
			emdSum += emds[i];
		}
		return (1/Math.sqrt(dimension))*emdSum;
	}	
	
	public static double getReduceEmd(double[] histA, double[] histB, double[] bins, int dimension) {
		return -1;
	}
	
	public static double getIndMinEmd(double[] histA, double[] histB, double[] bins, int dimension) {
		return -1;
	}
	
	public static double getDualEmd(double[] histA, double[] histB, double[] bins, int dimension) {
		DualBound dual = new DualBound(histA, histB, bins, dimension);
		double lowerA = dual.getKey(histA) + dual.getCKey(histB);
		double lowerB = dual.getKey(histB) + dual.getCKey(histA);
		return lowerA < lowerB ? lowerB : lowerA;
	}
	
	
	private static TreeMap<Double, Double> getDiscreteCDF (double [] hist, double[] bins) {
		TreeMap<Double, Double> cdf = new TreeMap<Double, Double>();
		for (int i = 0; i < bins.length; i++) {
			putWeightToBin(cdf, bins[i], hist[i]);
		}
		convertToCDF(cdf);
		return cdf;
	}
	
	private static TreeMap<Double, Double> getDiscreteCDF (Double [] hist, double[] bins) {
		TreeMap<Double, Double> cdf = new TreeMap<Double, Double>();
		for (int i = 0; i < bins.length; i++) {
			putWeightToBin(cdf, bins[i], hist[i]);
		}
		convertToCDF(cdf);
		return cdf;
	}	
	
	/**
	 * To get the discrete CDF of a histogram as a distribution
	 * @param hist the weights of the histogram, length = # of bins
	 * @param bins the bins of the histogram, length = # of bins * dimension
	 * @return a CDF as an object in class TreeMap<Double, Double> where each entry represents a bin
	 */
	public static TreeMap<Double, Double> getDiscreteCDFNormalized (double [] hist, double[] bins) {
		TreeMap<Double, Double> cdf = getDiscreteCDF(hist, bins);
		normalizeCDF(cdf);
		return cdf;
	}
	
	public static TreeMap<Double, Double> getDiscreteCDFNormalized (Double [] hist, double[] bins) {
		TreeMap<Double, Double> cdf = getDiscreteCDF(hist, bins);
		normalizeCDF(cdf);
		return cdf;
	}	

	
	private static double getDiscreteCDFAreaBetween(TreeMap<Double, Double> cdf, double keyA, double keyB) {
		double area = 0.0;
		if (keyA != keyB && keyA < keyB) {
			boolean containA = cdf.keySet().contains(keyA);
			boolean containB = cdf.keySet().contains(keyB);
			if (!containA && keyA > cdf.firstKey()) {
				area += cdf.get(cdf.lowerKey(keyA)) * (cdf.higherKey(keyA) - keyA);
				keyA = cdf.higherKey(keyA);
			}
			if (!containB && keyB < cdf.lastKey()) {
				area += cdf.get(cdf.lowerKey(keyB)) * (keyB - cdf.lowerKey(keyB));
				keyB = cdf.lowerKey(keyB);
			}
			for (Double key : cdf.keySet()) {
				if (key >= keyA && key < keyB && key != cdf.lastKey()) {
					area += cdf.get(key)*(cdf.higherKey(key) - key);
				}
			}
		}
		return area;
	}
	
	/**
	 * To approximate a histogram as a distribution using a normal distribution
	 * @param hist the weights of the histogram, length = # of bins
	 * @param bins the bins of the histogram, length = # of bins * dimension
	 * @return a normal distribution object in class NormalDistributionImpl
	 */
	// http://en.wikipedia.org/wiki/Variance
	public static NormalDistributionImpl getNormal(double[] hist, double[] bins) {
		double [] normalizedHist = normalizeArray(hist);
		double mean = 0.0;
		for (int i = 0; i < bins.length; i++) {
			mean += normalizedHist[i] * bins[i];
		}
		double variance = 0.0;
		for (int i = 0; i < bins.length; i++) {
			variance += Math.pow(bins[i], 2) * normalizedHist[i];
			//variance += bins[i] * Math.pow((normalizedHist[i] - mean), 2);
		}
		variance -= Math.pow(mean, 2);
		// standard deviation cannot be zero
		if (variance == 0) {
			variance = epsilon;
		}
		logger.setLevel(Level.DEBUG);
		if (variance < 0) {
			logger.debug(FormatUtil.toString(hist));
			logger.debug(FormatUtil.toString(bins));
			logger.debug(variance);
			variance = epsilon;
		}
		return new NormalDistributionImpl(mean, Math.sqrt(variance));
	}
	
	/**
	 * Using GaussianFitter to fit data to a normal function
	 * @param hist
	 * @param bins
	 * @return
	 */
//	public static NormalDistributionImpl getNormal(double[] hist, double[] bins) {
//		GaussianFitter gaussianFitter = new GaussianFitter(new LevenbergMarquardtOptimizer());
//		for(int i = 0; i < hist.length; i++) {
//			gaussianFitter.addObservedPoint(bins[i], hist[i]);
//		}
//		double[] result = gaussianFitter.fit();
//		return new NormalDistributionImpl(result[1], result[2]);
//	}
	
	public static double getNormalCDFAreaBetween(NormalDistributionImpl normal, double keyA, double keyB) {
		double area = 0.0;
//		try {
//			area = keyB*normal.cumulativeProbability(keyB) + normal.density(keyB)
//					- keyA*normal.cumulativeProbability(keyA) - normal.density(keyA);
//		} catch (MathException e) {
//			e.printStackTrace();
//		}
		try {
			double tKeyA = (keyA - normal.getMean()) / normal.getStandardDeviation();
			double tKeyB = (keyB - normal.getMean()) / normal.getStandardDeviation();
			area = tKeyB * stdNormal.cumulativeProbability(tKeyB) + stdNormal.density(tKeyB)
					- tKeyA * stdNormal.cumulativeProbability(tKeyA) - stdNormal.density(tKeyA);
			area = area * normal.getStandardDeviation();
 		} catch (MathException e) {
 			e.printStackTrace();
 		}
		return area;
	}
	
	public static double getNormalCDFIntersection(double[] normalA, double[] normalB) {
		return (normalA[0]*normalB[1] - normalB[0]*normalA[1])/(normalB[1] - normalA[1]);
	}
	
	public static double getNormalCDFIntersection(NormalDistributionImpl normalA, NormalDistributionImpl normalB) {
		return (normalA.getMean()*normalB.getStandardDeviation() - normalB.getMean()*normalA.getStandardDeviation())
				/(normalB.getStandardDeviation() - normalA.getStandardDeviation());
	}
	
	/**
	 * To get the min / max errors between the normal and the discrete cdf
	 * @param normal a normal distribution
	 * @param cdf a discrete cdf
	 * @param numIntervals the number of intervals
	 * @return a list of min / max errors, length % 2 = 0
	 */
	public static List<Double> getMinMaxError (NormalDistributionImpl normal, TreeMap<Double, Double> cdf, int numIntervals) {
		ArrayList<Double> errors = new ArrayList<Double>();
		double offset = cdf.firstKey();
		double intervalLength = (cdf.lastKey() - cdf.firstKey())/numIntervals;
		for (int i = 0; i < numIntervals; i++) {
			double start = offset + i * intervalLength;
			double end = offset + (i+1) * intervalLength;
			double [] minMaxErr = getMinMaxErrorInInterval(normal, cdf, start, end);
			errors.add(minMaxErr[0]);
			errors.add(minMaxErr[1]);
		}
		return errors;
	}
	
	private static double[] getMinMaxErrorInInterval(NormalDistributionImpl normal, TreeMap<Double, Double> cdf,
											double start, double end) {
		double [] minMaxErr = {Double.MAX_VALUE, -Double.MAX_VALUE};
		if (start < end) {
			TreeSet<Double> intersections = findAllIntersections(normal, cdf, start, end);
			for (Double intersection : intersections) {
				double error = getErrorAt(normal, cdf, intersection);
				minMaxErr = updateMinMaxErr(error, minMaxErr);
			}
		}
		else {
			System.out.println("illegal interval: " + start + " - " + end);
		}
		return minMaxErr;
	}
	
	// need compute for every intersection point
	private static double getErrorAt(NormalDistributionImpl normal, TreeMap<Double, Double> cdf, double pt) {
		double before = 0.0;
		double after = 0.0;
		before = getDiscreteCDFAreaBetween(cdf, cdf.firstKey(), pt) - getNormalCDFAreaBetween(normal, cdf.firstKey(), pt);
		after = getDiscreteCDFAreaBetween(cdf, pt, cdf.lastKey()) - getNormalCDFAreaBetween(normal, pt, cdf.lastKey()); 
		return before - after;
	}	
	
	private static TreeSet<Double> findAllIntersections(NormalDistributionImpl normal, TreeMap<Double, Double> cdf, 
			double start, double end) {
		TreeSet<Double> intersections = new TreeSet<Double>();
		intersections.add(start);
		intersections.add(end);
		for (Double key : cdf.keySet()) {
			if (start < key && key < end) {
			intersections.add(key);
				try {
					double potential = normal.inverseCumulativeProbability(cdf.get(key));
					if (potential > key && potential < end && potential < cdf.lastKey() && potential < cdf.higherKey(key)) {
						intersections.add(potential);
					}
				} catch (MathException e) {
					e.printStackTrace();
				}
			}
		}
		return intersections;
	}
	
	
	public static double lookupError(List<Double> minMaxError, boolean min, double start, double end, double point) {
		int numIntervals = minMaxError.size() / 2;
		double lengthInterval = (end-start) / numIntervals;
		int interval = (int)Math.floor((point - start)/lengthInterval);
		double error = 0.0;
		if (min) {
			error = minMaxError.get(interval*2);
		}
		else {
			error = minMaxError.get(interval*2+1);
		}
		return error;
	}
	
	public static double getFullError(NormalDistributionImpl normal, TreeMap<Double, Double> cdf, double start, double end) {
		return getDiscreteCDFAreaBetween(cdf, start, end) - getNormalCDFAreaBetween(normal, start, end);
	}
	
	
	private static double[] updateMinMaxErr(double error, double[] minMaxErr) {
		if (minMaxErr[0] > error) {
			minMaxErr[0] = error;
		}
		if (minMaxErr[1] < error) {
			minMaxErr[1] = error;
		}
		return minMaxErr;
	}
	
	private static void putWeightToBin(SortedMap<Double, Double> hist, double bin, double weight) {
		if (null != hist) {
			if (hist.containsKey(bin)) {
				hist.put(bin, hist.get(bin) + weight);
			}
			else {
				hist.put(bin, weight);
			}
		}
	}	
	
	private static void convertToCDF(TreeMap<Double, Double> hist) {
		for (Double key : hist.keySet()) {
			double val = hist.get(key);
			if (key != hist.firstKey()) {
				val += hist.get(hist.lowerKey(key));
			}
			hist.put(key, val);
		}
	}
	
	private static void normalizeCDF(TreeMap<Double, Double> hist) {
		double sum = hist.get(hist.lastKey());
		for (double key : hist.keySet()) {
			hist.put(key, hist.get(key) / sum);
		}
	}
	
	public static double[] randomHist(double[] hist) {
		for (int i = 0; i < hist.length; i++) {
			hist[i] = random.nextDouble()*100;
		}
		return hist;
	}
	
	public static double[] randomBin(double[] bins) {
		HashSet<Double> exist = new HashSet<Double>();
		for (int i = 0; i < bins.length; i++) {
			double bin = random.nextDouble()*100;
			while(exist.contains(bin)) {
				bin = random.nextDouble()*100;
			}
			bins[i] = bin;
			exist.add(bins[i]);
		}
		return bins;
	}
	
	public static double[] projectBins(double[] bins, int dimension, double[] vector) {
		double[] projections = new double[bins.length/dimension];
		vector = unitArray(vector);
		if (vector.length == dimension) {
			for (int i = 0; i < projections.length; i++) {
				for (int j = 0; j < dimension; j++) {
					projections[i] += bins[i*dimension+j]*vector[j];
				}
			}
		}
		return projections;
	}
	
	public static double[] projectBins(double[] bins, int dimension, double[] vector, int numVector) {
		double[] projections = new double[bins.length / dimension * numVector];
		for (int i = 0; i < numVector; i++) {
			double[] eachProjection = projectBins(bins, dimension, FormatUtil.getSubArray(vector, dimension * i, dimension * (i+1) - 1));
			for (int j = 0; j < bins.length / dimension; j++) {
				projections[i*bins.length / dimension + j] = eachProjection[j];
			}
		}
		return projections;
	}
	
	public static double[] normalizeArray(double[] hist) {
		double sum = 0.0;
		for (int i = 0; i < hist.length; i++) {
			sum += hist[i];
		}
		if (sum > 0.0d) {
			for (int i = 0; i < hist.length; i++) {
				hist[i] = hist[i] / sum;
			}
		}
		return hist;
	}
	
	public static double[] unitArray(double[] hist) {
		double sum = 0.0;
		for (int i = 0; i < hist.length; i++) {
			sum += Math.pow(hist[i], 2);
		}
		sum = Math.sqrt(sum);
		for (int i = 0; i < hist.length; i++) {
			hist[i] = hist[i] / sum;
		}
		return hist;
	}
	
	private static double[] get3DOrthogonal(double[] vector) {
		Vector3D v = new Vector3D(vector);
		Vector3D vo = v.orthogonal();
		return vo.toArray();
	}
	
	private static double[] get3DOrthogonal(double[] vector, double[] orvector) {
		Vector3D v = new Vector3D(vector);
		Vector3D orv = new Vector3D(orvector);
		Vector3D original = new Vector3D(0, 0, 0);
		Plane plane = new Plane(v, orv, original);
		return plane.getNormal().toArray();
	}	
	
	public static double getMinIn(double[] values) {
		double min = Double.MAX_VALUE;
		for (double val : values) {
			if (min > val) {
				min = val;
			}
		}
		return min;
	}
	
	public static double getMaxIn(double[] values) {
		double max = -Double.MAX_VALUE;
		for (double val: values) {
			if (max < val) {
				max = val;
			}
		}
		return max;
	}
	
	public static double getMaxFlow(double[] hist, double[] bins, int dimension) {
		double[] farthest = new double[bins.length / dimension];
		List<Double> listA = new ArrayList<Double>();
		List<Double> listB = new ArrayList<Double>();
		for (int i = 0; i < farthest.length; i++) {
			double max = -Double.MAX_VALUE;
			double[] binA = FormatUtil.getNthSubArray(bins, dimension, i);
			for (int j = 0; j < binA.length; j++) listA.add(binA[j]);
			for (int j = 0; j < farthest.length; j++) {
				double[] binB = FormatUtil.getNthSubArray(bins, dimension, j);
				for (int k = 0; k < binB.length; k++) listB.add(binB[k]);
				double dist = DistanceUtil.getGroundDist(listA, listB, DistanceType.LTWO, null);
				max = dist > max ? dist : max;
				listB.clear();
			}
			farthest[i] = max; 
			listA.clear();
		}
		
		double flow = 0.0;
		for (int i = 0; i < hist.length; i++) {
			flow += hist[i] * farthest[i];
		}
		
		return flow;
		
	}
	
	public static double getFlowBetween(double[] histA, Double[] histB, double[] bins, int dimension) {
		// sort bins in a descending order and flows to the nearest non-full bins
		double[] flowHist = new double[histA.length];
		double flow = 0.0;
		putBins(histA, weightsA);
		putBins(histB, weightsB);
		
		if (null == groundDistance) {
			groundDistance = getBinDistances(bins, dimension);
		}
		
		Iterator<Bin> it = weightsA.iterator();
		while(it.hasNext()) {
			Bin bin = it.next();
			double weight = bin.getWeight();
			int sourceIndex = bin.getIndex();
			TreeSet<BinDistance> dists = groundDistance.get(Integer.valueOf(sourceIndex));
			Iterator<BinDistance> itDist = dists.iterator();
			while(itDist.hasNext() && weight > 0) {
				BinDistance binDist = itDist.next();
				int distIndex = binDist.getIndex();
				double space = histB[distIndex] - flowHist[distIndex];
				if (space > 0) {
					double validFlow = weight > space ? space : weight;
					flow += validFlow * binDist.getDist();
					weight -= validFlow;
					flowHist[distIndex] += validFlow;
					
				}
			}
		}
		
		return flow;
	}
	
	public static double getKEmd(List<Double[]> sets, double[] bins, int dimension, int paraK) {
		int size = sets.size();
		List<Double> all = new ArrayList<Double>();
		for (int i = 0; i < size; i++) {
			for (int j = i+1; j < size; j++) {
				Double emd = DistanceUtil.getEmdLTwo(FormatUtil.toDoubleArray(sets.get(i)), FormatUtil.toDoubleArray(sets.get(j)), dimension, bins);
				all.add(emd);
			}
		}
		Collections.sort(all);
		return all.size() >= paraK ? all.get(paraK-1) : Double.MAX_VALUE;
	}
	
	private static void putBins(double[] histA, TreeSet<Bin> weights) {
		weights.clear();
		for (int i = 0; i < histA.length; i++) {
			Bin bin = new Bin(histA[i], i);
			weights.add(bin);
		}
	}
	
	private static void putBins(Double[] hist, TreeSet<Bin> weights) {
		weights.clear();
		for (int i = 0; i < hist.length; i++) {
			Bin bin = new Bin(hist[i], i);
			weights.add(bin);
		}
	}
	
	private static HashMap<Integer, TreeSet<BinDistance>> getBinDistances(double[] bins, int dimension) {
		int numBins = bins.length / dimension;
		HashMap<Integer, TreeSet<BinDistance>> map = new HashMap<Integer, TreeSet<BinDistance>>();
		for (int i = 0; i < numBins; i++) {
			TreeSet<BinDistance> each = new TreeSet<BinDistance>();
			double[] binA = FormatUtil.getNthSubArray(bins, dimension, i);
			for (int j = 0; j < numBins; j++) {
				double[] binB = FormatUtil.getNthSubArray(bins, dimension, j);
				double dist = 0.0;
				for (int k = 0; k < dimension; k++) {
					dist += Math.pow(binA[k] - binB[k], 2);
				}
				dist = Math.sqrt(dist);
				BinDistance binDist = new BinDistance(dist, j);
				each.add(binDist);
			}
			map.put(Integer.valueOf(i), each);
		}
		return map;
	}

	public static double[] substractAvg(double[] array) {
		double avg = HistUtil.avg(array);
		for (int i = 0; i < array.length; i++) {
			array[i] = array[i] - avg;
		}
		return array;
	}

	public static double sum(double[] array) {
		double sum = 0;
		for (double val : array) {
			sum += val;
		}
		return sum;
	}

	public static double avg(double[] array) {
		double sum = 0;
		for (double val : array) {
			sum += val;
		}
		return sum / array.length;
	}

	public static double minIn(double[] array) {
		double min = Double.MAX_VALUE;
		for (double val : array) {
			min = min > val ? val : min;
		}
		return min;
	}

	public static double maxIn(double[] array) {
		double max = -Double.MAX_VALUE;
		for (double val : array) {
			max = max < val ? val : max;
		}
		return max;
	}
	
}
