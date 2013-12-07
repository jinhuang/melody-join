package com.iojin.melody.utils;

//import java.text.DecimalFormat;

import java.io.IOException;
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
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
/**
 * The Histogram Utility class
 * @author soone
 * 
 */
public class HistUtil {
	
	private static final double epsilon = 0.00000000001;
	private static final Random random = new Random();
	private static Logger logger = Logger.getLogger(HistUtil.class);
	private static NormalDistributionImpl stdNormal = new NormalDistributionImpl(0, 1);
	
	private static TreeSet<Bin> weightsA = new TreeSet<Bin>();
	private static TreeSet<Bin> weightsB = new TreeSet<Bin>();
	
	private static HashMap<Integer, TreeSet<BinDistance>> groundDistance = null;
	
	private static Percentile statistics = new Percentile();
	
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
		for (int i = 0; i < hist.length; i++) {
			hist[i] = hist[i] / sum;
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
	
	
	
 	public static void main(String [] args) throws IOException {
// 		double [] hist = {0.05922227386932682,0.05015219588933983,0.05015219588933983,0.0432162539046439,0.048018059894048774,0.06349054585990893,0.0832313038163512,0.09230138179633819,0.08483190581281949,0.11631041174336258,0.11844454773865364,0.15099012166684225,0.17659975361033492,0.2438250374620032,0.37827560516533976,0.5399364068086373,0.33879408925245524,0.34412942924068285,0.21501420152557393,0.17339854961739834,0.11737747974100811,0.12164575173159023,0.1056397317669073,0.09977085777985689,0.05922227386932682,0.05388693388109918,0.07522829383400974,0.06135640986461788,0.06562468185519998,0.03414617592465691,0.060289341866972344,0.029344369935252027};
// 		System.out.println(FormatUtil.sum(hist));
// 		System.out.println(FormatUtil.toString(normalizeArray(hist)));
// 		System.out.println(FormatUtil.toString(normalizeArray(hist)));
// 		double[] hist19361 = {0.193035, 0.154083, 0.193346, 0.156367, 0.052397, 0.086980,
// 				0.055209, 0.092814, 0.000110, 0.000941, 0.000942, 0.000008, 
// 				0.004480, 0.005731, 0.000521, 0.000837, 0.000005, 0.000312, 
// 				0.000007, 0.000000, 0.000000, 0.001875, 0.000000, 0.000000,
// 				0.000000, 0.000104, 0.000000, 0.000000, 0.000000, 0.000000, 
// 				0.000000, 0.000000};
// 		double[] hist19580 = {0.195751, 0.148861, 0.191064, 0.162926, 0.044170, 0.085227, 0.049483, 0.081265, 0.001563, 0.004896, 0.005208, 0.002396, 0.007813, 0.009792, 0.004271, 0.003125, 0.000000, 0.000208, 0.000000, 0.000000, 0.000625, 0.000729, 0.000000, 0.000000, 0.000000, 0.000313, 0.000000, 0.000312, 0.000104, 0.000000, 0.000000, 0.000000};
// 		double[] hist19162 = {0.205735, 0.157401, 0.203663, 0.164692, 0.038031, 0.087312, 0.046256, 0.085333, 0.000212, 0.000000, 0.000001, 0.000000, 0.006042, 0.004896, 0.000105, 0.000000, 0.000006, 0.000000, 0.000001, 0.000000, 0.000001, 0.000208, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000208, 0.000000, 0.000000};
//		double[] hist15993 = {0.242613, 0.123229, 0.242096, 0.117604, 0.003558, 0.126796, 0.007826, 0.132422, 0.002500, 0.000000, 0.000000, 0.000000, 0.000105, 0.000001, 0.000104, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.001250, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000};
//		double[] hist16001 = {0.240956, 0.105108, 0.237823, 0.099792, 0.005528, 0.144397, 0.010641, 0.149192, 0.000208, 0.000000, 0.001250, 0.000000, 0.003334, 0.000521, 0.000312, 0.001042, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000};
// 		double[] bins = {0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 1, 0, 0, 1, 1, 0, 1, 2, 0, 1, 3, 1, 0, 0, 1, 0, 1, 1, 0, 2, 1, 0, 3, 1, 1, 0, 1, 1, 1, 1, 1, 2, 1, 1, 3, 2, 0, 0, 2, 0, 1, 2, 0, 2, 2, 0, 3, 2, 1, 0, 2, 1, 1, 2, 1, 2, 2, 1, 3, 3, 0, 0, 3, 0, 1, 3, 0, 2, 3, 0, 3, 3, 1, 0, 3, 1, 1, 3, 1, 2, 3, 1, 3 };
// 		double[][] vectors = {{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 0, 1}, {0, 1, 1}, {1, 1, 0}, {1, 1, 1}};
//		System.out.println("emd between: " + DistanceUtil.getEmdLTwo(hist15993, hist16001, 3, bins));
//		int numInterval = 5;
//		double[] errorRecord = new double[2 * numInterval + 1];
//		double[] errorGrid = new double[2 * numInterval + 2];
//		double[] histA = hist15993;
//		double[] histB = hist16001;
//		double[] recordA = new double[2];
//		double[] recordB = new double[2];
//		double[] gridBound = new double[4];
//		for (int i = 0 ; i < 7; i++) {
//			double[] projectedBins = HistUtil.projectBins(bins, 3, vectors[i], 1);
//			System.out.println("projected emd " + i + " : " + DistanceUtil.get1dEmd(histA, histB, projectedBins));
//			System.out.println("normal emd " + i + " : " + getNormalEmd(histA, histB, projectedBins, 5));
//			TreeMap<Double, Double> cdfA = getDiscreteCDF(histA, projectedBins);
//			TreeMap<Double, Double> cdfB = getDiscreteCDF(histB, projectedBins);
//			double tMin = cdfA.firstKey();
//			double tMax = cdfA.lastKey();
//			System.out.println("Absolute CDF difference: " + getProjectEmd(histA, histB, projectedBins));
//			System.out.println("Discrete CDF difference: " + (getDiscreteCDFAreaBetween(cdfA, tMin, tMax) - getDiscreteCDFAreaBetween(cdfB, tMin, tMax)));
//			System.out.println("Discrete A: " + getDiscreteCDFAreaBetween(cdfA, tMin, tMax));
//			System.out.println("Discrete B: " + getDiscreteCDFAreaBetween(cdfB, tMin, tMax));			
//			NormalDistributionImpl normalA = HistUtil.getNormal(histA, projectedBins);
//			double mA = 1/normalA.getStandardDeviation();
//			double bA = (-1)*normalA.getMean() /normalA.getStandardDeviation();
//			recordA[0] = mA;
//			recordA[1] = bA;
//			System.out.print("Record A: " + FormatUtil.toString(recordA) + "; ");
//			NormalDistributionImpl normalB = HistUtil.getNormal(histB, projectedBins);
//			double mB = 1/normalB.getStandardDeviation();
//			double bB = (-1) * normalA.getMean() /normalB.getStandardDeviation();
//			recordB[0] = mB;
//			recordB[1] = bB;
//			System.out.println("Record B: " + FormatUtil.toString(recordB));
//			double[] domain = new double[4];
//			double[] slopes = new double[2];
//			domain[0] = Math.min(mA, mB) - Math.abs(mB - mA)/2;
//			domain[1] = Math.max(mA, mB) + Math.abs(mB - mA)/2;
//			domain[2] = Math.min(bA, bB) - Math.abs(bB - bA) / 2;
//			domain[3] = Math.max(bB, bA) + Math.abs(bB - bA) / 2;
//			slopes[0] = FormatUtil.maxIn(projectedBins) * (-1);
//			slopes[1] = FormatUtil.minIn(projectedBins) * (-1);
//			Grid grid = new Grid(domain, slopes, 2);
//			TreeMap<Double, Double> cdfA = getDiscreteCDFNormalized(histA, projectedBins);
//			List<Double> errorA = HistUtil.getMinMaxError(normalA, cdfA, numInterval);
//			double fullErrorA = HistUtil.getFullError(normalA, cdfA,FormatUtil.minIn(projectedBins) , FormatUtil.maxIn(projectedBins));
//			TreeMap<Double, Double> cdfB = getDiscreteCDFNormalized(histB, projectedBins);
//			List<Double> errorB = HistUtil.getMinMaxError(normalB, cdfB, numInterval);
//			double fullErrorB = HistUtil.getFullError(normalB, cdfB,FormatUtil.minIn(projectedBins) , FormatUtil.maxIn(projectedBins));
//			
//			System.out.println();
//			System.out.println(grid);
//			
//			for (int j = 0; j < 2 * numInterval; j++) {
//				errorRecord[j] = errorA.get(j);
//				errorGrid[j] = errorB.get(j);
//			}
//			errorRecord[2 * numInterval] = fullErrorA;
//			errorGrid[2 * numInterval] = fullErrorB;
//			errorGrid[2 * numInterval + 1] = fullErrorB;
//			int gridId = (int) grid.getGridId(recordB);
//			gridBound = grid.getGrid(gridId, gridBound);
//			Direction direction = grid.locateRecordToGrid(recordA, gridBound);
//			double gridEmd = grid.getEmdBr(recordA, errorRecord, gridBound, errorGrid, direction, numInterval);
//			System.out.println("A record B grid: " + gridEmd);
//			
//			for (int j = 0; j < 2 * numInterval; j++) {
//				errorRecord[j] = errorB.get(j);
//				errorGrid[j] = errorA.get(j);
//			}
//			errorRecord[2 * numInterval] = fullErrorB;
//			errorGrid[2 * numInterval] = fullErrorA;
//			errorGrid[2 * numInterval + 1] = fullErrorA;
//			gridId = (int) grid.getGridId(recordA);
//			gridBound = grid.getGrid(gridId, gridBound);
//			direction = grid.locateRecordToGrid(recordB, gridBound);
//			gridEmd = grid.getEmdBr(recordB, errorRecord, gridBound, errorGrid, direction, numInterval);
//			System.out.println("B record A grid: " + gridEmd);	
//			System.out.println();
//		}
//		NormalDistributionImpl normal = new NormalDistributionImpl(-1.4996779501994864, 0.02281982762018056);
//		System.out.println("Area between 0 to 1 " + getNormalCDFAreaBetween(normal, 0, 1.5));
// 		int dimension = 3;
//		double[] vector = new double[dimension];
// 		vector = randomBin(vector);		
//		double[] orv = new double[dimension];
//		double[] thirdv = new double[dimension];
//		orv = get3DOrthogonal(vector);	
//		thirdv = get3DOrthogonal(vector, orv);	
//		System.out.println(FormatUtil.toString(vector));
//		System.out.println(FormatUtil.toString(orv));
//		System.out.println(FormatUtil.toString(thirdv));
 		
// 		int dimension = 3;
// 		int histLength = 32;
// 		double[] histA = {0.06637509647543093, 0.05402624131721122, 0.013377926421404682, 0.014664265500385902, 0.04064831489580654, 0.034731155132492926, 0.05788525855415487, 0.0645742217648572, 0.04167738615899151, -0.004116285052739903, 0.018266014921533315, 0.008232570105479805, 0.04759454592230512, 0.021095960895291997, 0.04142011834319527, 0.057627990738358635, 0.04347826086956522, 0.05994340108052483, 0.03293028042191922, -5.145356315924878E-4, 0.03730383329045537, 0.014664265500385902, 0.028556727553383076, -0.003859017236943658, -0.0020581425263699513, -5.145356315924878E-4, 0.06200154360689478, 0.035245690764085416, 0.003859017236943658, 0.04296372523797273, 0.024697710316439414, 0.043220993053768975};
// 		double[] histB = {0.05404089581304771, 0.008519961051606621, 0.037487828627069134, 0.01655306718597858, 0.007546251217137293, 0.02385589094449854, 0.019717624148003893, 0.037244401168451804, 0.015579357351509249, 0.02288218111002921, 0.030671859785783837, 0.035053554040895815, -0.004138266796494645, 0.035053554040895815, 0.023125608568646545, 0.047224926971762414, 0.026777020447906526, 0.037487828627069134, 0.05963972736124635, 0.05525803310613438, 0.012171372930866602, 0.01436222005842259, 0.005598831548198637, 0.054771178188899705, 0.06256085686465433, 0.05111976630963972, 0.04625121713729309, 0.04576436222005842, 0.036027263875365145, 0.03919182083739046, 0.03140214216163584, 0.011197663096397274};
// 		double[] bins = {41,0,61,54,8,40,16,10,27,33,16,57,54,11,47,47,11,54,5,43,18,62,33,8,39,6,59,39,37,44,18,47,15,23,31,58,20,11,44,29,62,8,44,6,19,20,39,47,16,43,56,18,53,38,29,36,17,7,35,6,10,28,38,34,58,6,62,48,35,40,23,61,14,31,9,58,20,0,20,36,13,58,37,9,13,14,4,26,15,61,38,46,34,7,11,46};
// 		System.out.println(DistanceUtil.getEmdLTwo(histA, histB, dimension, bins));
// 		double[] histA = new double[histLength];
// 		double[] histB = new double[histLength];
// 		double[] histC = new double[histLength];
// 		double[] bins = new double[dimension * histLength];
// 		
//	 	histA = normalizeArray(randomHist(histA));
//		histB = normalizeArray(randomHist(histB));
//		histC = normalizeArray(randomHist(histC));
//		bins = randomBin(bins);
//		int testRound = 100;
//		
//		double maxRatio = -Double.MAX_VALUE;
//		double minRatio = Double.MAX_VALUE;
//		double emdMaxRatio = -Double.MAX_VALUE;
//		double emdMinRatio = Double.MAX_VALUE;
//		double emdSum = 0.0;
//		double emdBoundSum = 0.0;
//		double flowSum = 0.0;
		
//		for (int i = 0; i < testRound; i++) {
//		 	histA = normalizeArray(randomHist(histA));
//			histB = normalizeArray(randomHist(histB));
//			histC = normalizeArray(randomHist(histC));
//			double flow = getFlowBetween(histA, FormatUtil.toObjectDoubleArray(histC), bins, dimension) + getFlowBetween(histB, FormatUtil.toObjectDoubleArray(histC), bins, dimension);
//			double emdBound = DistanceUtil.getEmdLTwo(histA, histC, dimension, bins) + DistanceUtil.getEmdLTwo(histC, histB, dimension, bins);;
//			double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
//			flowSum += flow;
//			emdSum += emd;
//			emdBoundSum += emdBound;
//			double ratio = flow / emd;
//			maxRatio = maxRatio > ratio ? maxRatio : ratio;
//			minRatio = minRatio > ratio ? ratio : minRatio;
//			ratio = emdBound / emd;
//			emdMaxRatio = emdMaxRatio > ratio ? emdMaxRatio : ratio;
//			emdMinRatio = emdMinRatio > ratio ? ratio : emdMinRatio;
//		}
//		System.out.println("Bound in " + testRound + " : max " + emdMaxRatio + " ; min " + emdMinRatio + " ; avg " + emdBoundSum / emdSum);
//		System.out.println("Flow in " + testRound + " : max " + maxRatio + " ; min " + minRatio + " ; avg " + flowSum / emdSum);
	
//		double maxA = getMaxFlow(histA, bins, dimension);
//		double maxB = getMaxFlow(histB, bins, dimension);
//		double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
		
//		System.out.println("Emd: " + emd + " ; maxA: " + maxA + " ; maxB: " + maxB);
// 		int testRound = 500;
//// 		int counter = 0;
// 		double normalSum = 0.0;
// 		double realSum = 0.0;
// 		double projectSum = 0.0;
// 		double dualSum = 0.0;
// 		double indminSum = 0.0;
// 		double minNormal = Double.MAX_VALUE;
// 		double maxNormal = 0.0;
// 		double minProject = Double.MAX_VALUE;
// 		double maxProject = 0.0;
// 		double minDual = Double.MAX_VALUE;
// 		double maxDual = 0.0;
// 		double minIndMin = 0.0;
// 		double maxIndMin = 0.0;
// 		bins = randomBin(bins);
// 		int dualNumber = 7;
// 		DualBound[] dual = new DualBound[dualNumber];
// 		for (int i = 0; i < dualNumber; i++) {
// 	 		histA = normalizeArray(randomHist(histA));
// 			histB = normalizeArray(randomHist(histB));
// 			dual[i] = new DualBound(histA, histB, bins, dimension);
// 		}
// 		
// 		int counter = 0;
// 		
//// 		for (int i = 0; i < testRound; i++) {
//// 			histA = normalizeArray(randomHist(histA));
//// 			histB = normalizeArray(randomHist(histB));
//// 			
//// 			
//// 			double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
//// 			double projectEmd = HistUtil.getProjectEmd(histA, histB, bins, dimension);
//// 			double normalEmd = HistUtil.getNormalEmd(histA, histB, bins, 5, dimension);
//// 			//double dualEmd = HistUtil.getDualEmd(histA, histB, bins, dimension);
//// 			double[] dualEmds = new double[dualNumber];
//// 			for (int j = 0; j < dualNumber; j++) {
//// 				dualEmds[j] = dual[j].getDualEmd(histA, histB);
//// 			}
//// 			double dualEmd = FormatUtil.maxIn(dualEmds);
//// 			double indminEmd = DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null);
//// 			
//// 			realSum += emd;
//// 			projectSum += projectEmd;
//// 			normalSum += normalEmd;
//// 			dualSum += dualEmd;
//// 			indminSum += indminEmd;
//// 			
//// 			double normalRatio =  normalEmd / emd;
//// 			minNormal = normalRatio > minNormal ? minNormal : normalRatio;
//// 			maxNormal = normalRatio > maxNormal ? normalRatio : maxNormal;
//// 			double projectRatio = projectEmd / emd;
//// 			minProject = projectRatio > minProject ? minProject : projectRatio;
//// 			maxProject = projectRatio > maxProject ? projectRatio : maxProject;
//// 			double dualRatio = dualEmd / emd;
//// 			minDual = dualRatio > minDual ? minDual : dualRatio;
//// 			maxDual = dualRatio > maxDual ? dualRatio : maxDual;
//// 			
//// 			if (dualEmd > normalEmd) {
//// 				counter++;
//// 			}
//// 		}
// 		
// 		
//// 		System.out.println("Projection: " + (projectSum / realSum) * 100 + "%" + " min : " + minProject + " max: " + maxProject);
//// 		System.out.println("Normal: " + (normalSum / realSum) * 100 + "%" + " min : " + minNormal + " max: " + maxNormal);
//// 		System.out.println("Dual: " + (dualSum / realSum) * 100 + "%");
//// 		System.out.println("Independent minimization: " + (indminSum / realSum) * 100 + "%");
//// 		System.out.println("Dual vs. Normal: " + counter * 100/testRound + "%" + " min: " + minDual + " max: " + maxDual);
// 		int realCounter = 0;
// 		int pruneCounter = 0;
// 		long emdTimer = 0;
// 		long indminTimer = 0;
// 		long normalTimer = 0;
// 		long dualTimer = 0;
// 		for (int i = 0; i < testRound; i++) {
// 			histA = normalizeArray(randomHist(histA));
// 			histB = normalizeArray(randomHist(histB));			
// 			long tmp = System.nanoTime();
// 			double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
// 			
// 			emdTimer += System.nanoTime() - tmp;
// 			
// 			tmp = System.nanoTime();
// 			double indminEmd = DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LTWO, null);
// 			indminTimer += System.nanoTime() - tmp;
// 			
// 			tmp = System.nanoTime();
// 			double[] dualEmds = new double[dualNumber];
// 			for (int j = 0; j < dualNumber; j++) {
// 				dualEmds[j] = dual[j].getDualEmd(histA, histB);
// 			}
// 			double dualEmd = FormatUtil.maxIn(dualEmds);
// 			dualTimer += System.nanoTime() - tmp;
// 			
// 			tmp = System.nanoTime();
// 			double normalEmd = HistUtil.getNormalEmd(histA, histB, bins, 5, dimension);
// 			normalTimer += System.nanoTime() - tmp;
// 			
// 			realSum += emd;
// 			normalSum += normalEmd;
// 			dualSum += dualEmd;
// 			indminSum += indminEmd;
// 			
// 			double normalRatio =  normalEmd / emd;
// 			minNormal = normalRatio > minNormal ? minNormal : normalRatio;
// 			maxNormal = normalRatio > maxNormal ? normalRatio : maxNormal;
// 			double dualRatio = dualEmd / emd;
// 			minDual = dualRatio > minDual ? minDual : dualRatio;
// 			maxDual = dualRatio > maxDual ? dualRatio : maxDual; 
// 			
// 			if (indminEmd > 0.1*emd) {
// 				pruneCounter++;
// 			}
// 		}
// 		
// 		System.out.println("Computed " + testRound + "avg Emd: " + realSum / testRound);
// 		System.out.println("exact in " + emdTimer / 1000000 + "ms");
// 		System.out.println("indepedent min in: " + indminTimer / 1000000 + " ms " + indminSum / realSum * 100 + "%");
// 		System.out.println("dual in: " + dualTimer / 1000000 + " ms " + dualSum / realSum * 100 + "%");
// 		System.out.println("normal in: " + normalTimer / 1000000 + " ms " + normalSum / realSum * 100 + "%");
// 		
// 		System.out.println(pruneCounter);
 		int length = 12;
 		int dimension = 3;
 		double[] bins = new double[length * dimension];
 		bins = randomBin(bins);
 		int sample = 100;
 		double[][] histSample = new double[sample][length];
 		for (int i = 0; i < sample; i++) {
 			histSample[i] = normalizeArray(randomHist(new double[length]));
 		}
 		int round = sample * (sample - 1) /2;
 		double[] emdCollection = new double[round];
 		double[] euclideanCollection = new double[round];
// 		double[] loneCollection = new double[round];
 		int counter = 0;
 		for (int i = 0; i < sample; i++) {
 			for (int j = i + 1; j < sample; j++) {
 				emdCollection[counter] = DistanceUtil.getEmdLTwo(histSample[i], histSample[j], dimension, bins);
 				euclideanCollection[counter] = DistanceUtil.getLTWODist(histSample[i], histSample[j]);
 				counter++;
 			}
 		}
// 		
 		statistics.setData(emdCollection);
 		System.out.println("0% :" + statistics.evaluate(0.00001));
 		System.out.println("1% :" + statistics.evaluate(1));
 		System.out.println("10% :" + statistics.evaluate(10));
 		System.out.println("20% :" + statistics.evaluate(20));
 		System.out.println("30% :" + statistics.evaluate(30));
 		System.out.println("40% :" + statistics.evaluate(40));
 		System.out.println("50% :" + statistics.evaluate(50));
 		System.out.println("60% :" + statistics.evaluate(60));
 		System.out.println("70% :" + statistics.evaluate(70));
 		System.out.println("80% :" + statistics.evaluate(80));
 		System.out.println("90% :" + statistics.evaluate(90));
 		System.out.println("100% :" + statistics.evaluate(100));	
 		
 		FileUtil.writeContent(FormatUtil.formatDoubleArray(emdCollection));
 		FileUtil.writeContent(FormatUtil.formatDoubleArray(euclideanCollection));
 		System.exit(0);
	}
	
}
