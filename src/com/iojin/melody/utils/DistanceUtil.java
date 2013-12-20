package com.iojin.melody.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.NonNegativeConstraint;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import com.iojin.melody.utils.HistUtil;



public class DistanceUtil {
	
	public static final double EPSILON = 1.0e-4;
	private static final int maxUlps = 10;
	
	private static PointValuePair solution;
	private static SimplexSolver solver = new SimplexSolver(EPSILON, maxUlps);
	private final static MaxIter maxIter = new MaxIter(5000);
	
	/**
	 * Compute the 1 dimensional EMD between two histograms, assuming bins follow
	 * a sequence of non-negative integers, i.e., 0, 1, 2, 3, 4, ...
	 * @param histA the first histogram
	 * @param histB the second histogram
	 * @param numBins the number of bins in the histogram
	 * @return the 1 dimensional EMD
	 */
	public static double get1dEmd(double[] histA, double[] histB, int numBins) {
		double emd = 0;
		double total = 0;
		for (int i = 0; i < numBins; i++) {
			emd = histA[i] + emd - histB[i];
			total += emd > 0 ? emd : emd * (-1);
		}
		return total;
	}
	
	/**
	 * Compute the 1 dimensional EMD between two histograms, specifying the bins
	 * @param histA the first histogram
	 * @param histB the second histogram
	 * @param bins the bins
	 * @return the 1 dimensional EMD
	 */
	public static double get1dEmd(double[] histA, double[] histB, double[]bins) {
		double total = 0.0;
		TreeMap<Double, Double> hA = HistUtil.getDiscreteCDFNormalized(histA, bins);
		TreeMap<Double, Double> hB = HistUtil.getDiscreteCDFNormalized(histB, bins);
		double key = hA.firstKey();
		while (null != hA.higherEntry(key)) {
			total += Math.abs(hA.get(key) - hB.get(key)) * (hA.higherKey(key) - key);
			key = hA.higherKey(key);
		}
		return total; 
	}
	
	public static double get1dEmd(double[] histA, Double[] histB, double[]bins) {
		double total = 0.0;
		TreeMap<Double, Double> hA = HistUtil.getDiscreteCDFNormalized(histA, bins);
		TreeMap<Double, Double> hB = HistUtil.getDiscreteCDFNormalized(histB, bins);
		double key = hA.firstKey();
		while (null != hA.higherEntry(key)) {
			total += Math.abs(hA.get(key) - hB.get(key)) * (hA.higherKey(key) - key);
			key = hA.higherKey(key);
		}
		return total; 
	}	
	
	public static double getEmdLTwo(double[] histA, double[] histB, int dimension, double[] bins) {
		return getEmd(histA, histB, dimension, bins, DistanceType.LTWO, null);
	}
	
	public static double getEmdLOne(double[] histA, double[] histB, int dimension, double[] bins) {
		return getEmd(histA, histB, dimension, bins, DistanceType.LONE, null);
	}
	
	public static double getEmdLThree(double[] histA, double[] histB, int dimension, double[] bins) {
		return getEmd(histA, histB, dimension, bins, DistanceType.LTHREE, null);
	}
	
	public static double getLONEDist(double[] histA, double[] histB) {
		double result = 0.0;
		for (int i = 0; i < histA.length; i++) {
			result += Math.abs(histA[i] - histB[i]);
		}
		return result;
	}

	// Using Apache Common Maths SimplexSolver
	/**
	 * Compute the EMD between two histograms, using Simplex method
	 * @param histA the first histogram
	 * @param histB the second histogram
	 * @param dimension the dimension of bins
	 * @param bins the bins of the histogram
	 * @param distType the ground distance type 
	 * @param costMatrix specific cost matrix used for ground distance, only valid if distType is ARBITRARY
	 * @return the exact EMD
	 */
	public static double getEmd(double[] histA, double[] histB, int dimension, double[] bins, DistanceType distType, HashMap<String, Double> costMatrix) {
		if (HistUtil.sum(histA) != HistUtil.sum(histB)) {
			//System.out.println("Infeasible: " + FormatUtil.toString(histA) + " - " + FormatUtil.toString(histB));
			histA = HistUtil.normalizeArray(histA);
			histB = HistUtil.normalizeArray(histB);
		}
		int numBins = bins.length / dimension;
		double[] coefficients = new double[numBins*numBins];
		if (numBins != histA.length || histA.length != histB.length) {
			return -1.0;
		}
		Collection<LinearConstraint> constraints = new ArrayList<LinearConstraint>();
		for (int i = 0; i < numBins; i++) {
			for (int j = 0; j < numBins; j++) {
				coefficients[i*numBins + j] = getGroundDist(getBin(bins, dimension, i), getBin(bins, dimension, j), distType, costMatrix);
			}
		}
		for (int i = 0; i < numBins; i++) {
			double[] flowFrom = getFlow(numBins, i, true);
			double[] flowTo = getFlow(numBins, i, false);
			constraints.add(new LinearConstraint(flowFrom, Relationship.LEQ, histA[i]));
			constraints.add(new LinearConstraint(flowTo, Relationship.LEQ, histB[i]));
			if (histA[i] - histB[i] > 0) {
				constraints.add(new LinearConstraint(subtract(flowFrom, flowTo), Relationship.EQ, histA[i] - histB[i]));
			}
			else {
				constraints.add(new LinearConstraint(subtract(flowTo, flowFrom), Relationship.EQ, histB[i] - histA[i]));
			}
		}
		LinearObjectiveFunction minF = new LinearObjectiveFunction(coefficients, 0);
		LinearConstraintSet constraintSet = new LinearConstraintSet(constraints);
		NonNegativeConstraint nonNegConstraint = new NonNegativeConstraint(true);
		solution = solver.optimize(minF, constraintSet, nonNegConstraint, GoalType.MINIMIZE, maxIter);
		return solution.getValue();
	}
	
	public static double getIndMinEmd(double[] histA, double[] histB, int dimension, double[] bins, DistanceType distType, HashMap<String, Double> costMatrix) {
		if (HistUtil.sum(histA) != HistUtil.sum(histB)) {
			//System.out.println("Infeasible: " + FormatUtil.toString(histA) + " - " + FormatUtil.toString(histB));
			histA = HistUtil.normalizeArray(histA);
			histB = HistUtil.normalizeArray(histB);
		}
		int numBins = bins.length / dimension;
		double[] coefficients = new double[numBins*numBins];
		if (numBins != histA.length || histA.length != histB.length) {
			return -1.0;
		}
		Collection<LinearConstraint> constraints = new ArrayList<LinearConstraint>();
		for (int i = 0; i < numBins; i++) {
			for (int j = 0; j < numBins; j++) {
				coefficients[i*numBins + j] = getGroundDist(getBin(bins, dimension, i), getBin(bins, dimension, j), distType, costMatrix);
			}
		}
		for (int i = 0; i < numBins; i++) {
			double[] flowFrom = getFlow(numBins, i, true);
			double[] flowTo = getFlow(numBins, i, false);
			constraints.add(new LinearConstraint(flowFrom, Relationship.LEQ, histA[i]));
//			constraints.add(new LinearConstraint(flowTo, Relationship.LEQ, histB[i]));
			if (histA[i] - histB[i] > 0) {
				constraints.add(new LinearConstraint(subtract(flowFrom, flowTo), Relationship.EQ, histA[i] - histB[i]));
			}
//			else {
//				constraints.add(new LinearConstraint(subtract(flowTo, flowFrom), Relationship.LEQ, histB[i] - histA[i]));
//			}
		}
		LinearObjectiveFunction minF = new LinearObjectiveFunction(coefficients, 0);
		LinearConstraintSet constraintSet = new LinearConstraintSet(constraints);
		NonNegativeConstraint nonNegConstraint = new NonNegativeConstraint(true);
		solution = solver.optimize(minF, constraintSet, nonNegConstraint, GoalType.MINIMIZE, maxIter);
		return solution.getValue();
	}	
	
	public static double getRubnerEmd(double[] histA, double[] histB, int dimension, double[] bins, DistanceType type) {
		double rubner = 0.0;
		int numBins = bins.length / dimension;
		for (int i = 0; i < dimension; i++) {
			double temp = 0.0;
			for (int j = 0; j < numBins; j++) {
				temp += histA[j] * bins[j * dimension + i] - histB[j] * bins[j * dimension + i];
			}
			switch(type) {
			case LONE:
				break;
			case LTWO:
				temp = Math.pow(temp, 2);
				break;
			case LTHREE:
				temp = Math.pow(Math.abs(temp), 3);
				break;
			default:
				break;
			}
			rubner += temp;
		}
		switch(type) {
		case LONE:
			break;
		case LTWO:
			rubner = Math.sqrt(rubner);
			break;
		case LTHREE:
			rubner = Math.cbrt(rubner);
			break;
		default:
			break;
		}		
		return rubner;
	}
	
	public static double[] getRubnerValue(double[] hist, int dimension, double[] bins) {
		double[] rVal = new double[dimension];
		int numBins = bins.length / dimension;
		for (int i = 0; i < dimension; i++) {
			for (int j = 0; j < numBins; j++) {
				rVal[i] += hist[j] * bins[j * dimension + i];
			}
		}
		return rVal;
	}
	
	public static double getRubnerBound(double[] rubnerValue, double[] cellRubner, int dimension) {
		if (dimension == 3) {
			double[] v1 = FormatUtil.getSubArray(cellRubner, 0, 3);
			double[] v2 = new double[3];
			v2[0] = cellRubner[3];
			v2[1] = cellRubner[1];
			v2[2] = cellRubner[2];
			double[] v3 = new double[3];
			v3[0] = cellRubner[3];
			v3[1] = cellRubner[4];
			v3[2] = cellRubner[2];
			double[] v4 = new double[3];
			v4[0] = cellRubner[0];
			v4[1] = cellRubner[4];
			v4[2] = cellRubner[2];
			double[] v5 = new double[3];
			v5[0] = cellRubner[0];
			v5[1] = cellRubner[1];
			v5[2] = cellRubner[5];
			double[] v6 = new double[3];
			v6[0] = cellRubner[3];
			v6[1] = cellRubner[1];
			v6[2] = cellRubner[5];
			double[] v7 = new double[3];
			v7[0] = cellRubner[3];
			v7[1] = cellRubner[4];
			v7[2] = cellRubner[5];
			double[] v8 = new double[3];
			v8[0] = cellRubner[0];
			v8[1] = cellRubner[4];
			v8[2] = cellRubner[5];
			
			double[] proj = new double[2];
			boolean []x = new boolean[3];
			boolean []y = new boolean[3];
			boolean []z = new boolean[3];
			if (rubnerValue[0] < cellRubner[0]) x[0] = true;
			else if (rubnerValue[0] > cellRubner[3]) x[2] = true;
			else x[1] = true;
			if (rubnerValue[1] < cellRubner[1]) y[0] = true;
			else if (rubnerValue[1] > cellRubner[4]) y[2] = true;
			else y[1] = true;
			if (rubnerValue[2] < cellRubner[2]) z[0] = true;
			else if (rubnerValue[2] > cellRubner[5]) z[2] = true;
			else z[1] = true;
			
			if (x[0] && y[0] && z[0]) {
				return getLTWODist(rubnerValue, v1);
			}
			else if (x[0] && y[0] && z[1]) {
				return getLTWODist(FormatUtil.getSubArray(rubnerValue, 0, 2), FormatUtil.getSubArray(v1, 0, 2));
			}
			else if (x[0] && y[0] && z[2]) {
				return getLTWODist(rubnerValue, v5);
			}
			else if ((x[0] && y[1] && z[0])) {
				
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v1[0];
				temp[1] = v1[2];
				return getLTWODist(proj, temp);
			}
			else if (x[0] && y[1] && z[1]) {
				return Math.abs(rubnerValue[0] - cellRubner[0]);
			}
			else if ((x[0] && y[1] && z[2])) {
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v5[0];
				temp[1] = v5[2];
				return getLTWODist(proj, temp);				
			}
			else if (x[0] && y[2] && z[0]) {
				return getLTWODist(rubnerValue, v4);
			}
			else if (x[0] && y[2] && z[1]) {
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[1];
				double[] temp = new double[2];
				temp[0] = v4[0];
				temp[1] = v4[1];
				return getLTWODist(proj, temp);
			}
			else if (x[0] && y[2] && z[2]) {
				return getLTWODist(rubnerValue, v8);
			}
			else if (x[1] && y[0] && z[0]) {
				proj[0] = rubnerValue[1];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v1[1];
				temp[1] = v1[2];
				return getLTWODist(proj, temp);
			}
			else if (x[1] && y[0] && z[1]) {
				return Math.abs(rubnerValue[1] - cellRubner[1]);
			}
			else if (x[1] && y[0] && z[2]) {
				proj[0] = rubnerValue[1];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v5[1];
				temp[1] = v5[2];
				return getLTWODist(proj, temp);				
			}
			else if (x[1] && y[1] && z[0]) {
				return Math.abs(rubnerValue[2] - cellRubner[2]);
			}
			else if (x[1] && y[1] && z[1]) {
				return 0.0;
			}
			else if (x[1] && y[1] && z[2]) {
				return Math.abs(rubnerValue[2] - cellRubner[5]);
			}
			else if (x[1] && y[2] && z[0]) {
				proj[0] = rubnerValue[1];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v4[1];
				temp[1] = v4[2];
				return getLTWODist(proj, temp);				
			}
			else if (x[1] && y[2] && z[1]) {
				return Math.abs(rubnerValue[1] - cellRubner[4]);
			}
			else if (x[1] && y[2] && z[2]) {
				proj[0] = rubnerValue[1];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v8[1];
				temp[1] = v8[2];
				return getLTWODist(proj, temp);
			}
			else if (x[2] && y[0] && z[0]) {
				return getLTWODist(rubnerValue, v2);
			}
			else if (x[2] && y[0] && z[1]) {
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[1];
				double[] temp = new double[2];
				temp[0] = v2[0];
				temp[1] = v2[1];
				return getLTWODist(proj, temp);
			}
			else if (x[2] && y[0] && z[2]) {
				return getLTWODist(rubnerValue, v6);
			}
			else if (x[2] && y[1] && z[0]) {
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v2[0];
				temp[1] = v2[2];
				return getLTWODist(proj, temp);
			}
			else if (x[2] && y[1] && z[1]) {
				return Math.abs(rubnerValue[0] - cellRubner[3]);
			}
			else if (x[2] && y[1] && z[2]) {
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[2];
				double[] temp = new double[2];
				temp[0] = v6[0];
				temp[1] = v6[2];
				return getLTWODist(proj, temp);
			}
			else if (x[2] && y[2] && z[0]) {
				return getLTWODist(rubnerValue, v3);
			}
			else if (x[2] && y[2] && z[1]) {
				proj[0] = rubnerValue[0];
				proj[1] = rubnerValue[1];
				double[] temp = new double[2];
				temp[0] = v3[0];
				temp[1] = v3[1];
				return getLTWODist(proj, temp);
			}
			else if (x[2] && y[2] && z[2]) {
				return getLTWODist(rubnerValue, v7);
			}
		}
		return 0.0;
	}
	
	public static double getLTWODist(double[] a, double[] b) {
		int dimension = a.length;
		double dist = 0.0;
		for (int i = 0; i < dimension; i++) {
			dist += Math.pow(a[i] - b[i], 2);
		}
		return Math.sqrt(dist);
	}	
	
	public static List<Double> getBin(double[] bins, int dimension, int number) {
		int numBins = bins.length / dimension;
		if (number < numBins) {
			ArrayList<Double> bin = new ArrayList<Double>();
			for (int i = number * dimension; i < (number+1)*dimension; i++) {
				bin.add(bins[i]);
			}
			return bin;
		}
		else return null;
	}
	
	public static double getGroundDist(List<Double> binA, List<Double> binB, DistanceType type, HashMap<String, Double> costMatrix) {
		if (binA.size() == binB.size()) {
			double dist = 0.0;
			switch (type) {
			default:
			case LTWO:
				double sum = 0.0;
				for (int i = 0; i < binA.size(); i++) {
					sum += Math.pow(binB.get(i) - binA.get(i),2);
				}
				dist = Math.sqrt(sum);
				break;
			case LONE:
				for (int i = 0; i < binA.size(); i++) {
					dist += Math.abs(binB.get(i) - binA.get(i));
				}
				break;
			case LTHREE:
				for (int i = 0; i < binA.size(); i++) {
					dist += Math.pow(binB.get(i) - binA.get(i), 3);
				}
				dist = Math.abs(Math.cbrt(dist));
				break;
			case ARBITRARY:
					if (null != costMatrix) {
						String key = convertBinToKey(binA) + ";" + convertBinToKey(binB);
						dist = costMatrix.get(key);
					}
					else dist = -1.0;
				break;
			}
			return dist;
		}
		else return -1.0;
	}
	
	private static String convertBinToKey(List<Double> bin) {
		String key = "";
		for (int i = 0; i < (bin.size()-1); i++) {
			key += String.valueOf(bin.get(i)) + ",";
		}
		key += String.valueOf(bin.get(bin.size()-1));
		return key;
	}
	
	private static double[] getFlow(int numBins, int number, boolean outer) {
		if (number < numBins * numBins) {
			double[] flow = new double[numBins*numBins];
			for (int i = 0; i < flow.length; i++) {
				if (outer && i >= number*numBins && i < (number+1)*numBins) {
					flow[i] = 1.0;
				}
				else if (!outer && number == i%numBins) {
					flow[i] = 1.0;
				}
				else {
					flow[i] = 0.0;
				}
			}
			return flow;
		}
		else return null;
	}	
	
	private static double[] subtract(double[] flowFrom, double[] flowTo) {
		if (flowFrom.length == flowTo.length) {
			double[] netFlow = new double[flowFrom.length];
			for (int i = 0; i < flowTo.length; i++) {
				netFlow[i] = flowFrom[i] - flowTo[i];
			}
			return netFlow;
		}
		else return null;
	}
}
