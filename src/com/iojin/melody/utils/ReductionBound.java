package com.iojin.melody.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class ReductionBound {
	private double[] reductionMatrix;
	private double[] reducedCostMatrix;
	private static Random random = new Random();
	
	public ReductionBound(int originalDimension, int reducedDimension, double[] bins) {
		
		double[] costMatrix = getCostMatrixDoubleArray(originalDimension, bins);
		
		reductionMatrix = new double[originalDimension * reducedDimension];
		
		for (int i = 0; i < originalDimension; i++) {
			reductionMatrix[i*reducedDimension + random.nextInt(reducedDimension)] = 1.0;
		}
		
		reducedCostMatrix = new double[reducedDimension * reducedDimension];
		
		for (int i = 0; i < reducedDimension; i++) {
			for (int j = 0; j < reducedDimension; j++) {
				int index = i * reducedDimension + j;
				double min = Double.MAX_VALUE;
				if (i == j) {
					reducedCostMatrix[index] = 0;
				}
				else {
					
					List<Integer> iList = new ArrayList<Integer>();
					List<Integer> jList = new ArrayList<Integer>();
					for (int k = 0; k < originalDimension; k++) {
						if (Math.abs(reductionMatrix[k * reducedDimension + i] - 1.0) < 0.00001) {
							iList.add(k);
						}
						if (Math.abs(reductionMatrix[k * reducedDimension + j] - 1.0) < 0.00001) {
							jList.add(k);
						}
					}
					for (Integer eachI : iList) {
						for (Integer eachJ : jList) {
							double dist =  costMatrix[eachI * originalDimension + eachJ];
							min = min > dist ? dist : min;
						}
					}
					reducedCostMatrix[index] = min;
					
				}
			}
		}
	}
	
	public double getReducedEmd(double[] histA, double[] histB) {
		double[] reducedHistA = reduce(histA);
		double[] reducedHistB = reduce(histB);
		int reducedDimension = (int) Math.sqrt(reducedCostMatrix.length);
		double[] bins = new double[reducedDimension];
		for (int i = 0; i < bins.length; i++) {
			bins[i] = i;
		}
		
 		return DistanceUtil.getEmd(reducedHistA, reducedHistB, 1, bins, DistanceType.ARBITRARY, getCostMatrix());
	}
	
	private double[] reduce (double[] hist) {
		int reducedDimension = reductionMatrix.length / hist.length;
		double[] reducedHist = new double[reducedDimension];
		for (int i = 0; i < hist.length; i++) {
			for (int j = 0; j < reducedDimension; j++) {
				reducedHist[j] += hist[i] * reductionMatrix[i * reducedDimension + j];
			}
		}
		return reducedHist;
	}
	
	private HashMap<String, Double> getCostMatrix() {
		HashMap<String, Double> matrix = new HashMap<String, Double> ();
		int reducedDimension = (int) Math.sqrt(reducedCostMatrix.length);
		for (int i = 0; i < reducedDimension; i++) {
			for (int j = 0; j < reducedDimension; j++) {
				matrix.put((double)i + ";" + (double)j, reducedCostMatrix[i * reducedDimension + j]);
			}
		}
		return matrix;
	}
	
	public static double[] getCostMatrixDoubleArray(int histLength,  double[] bins) {
		int dimension = bins.length / histLength;
 		double[] costMatrix = new double[histLength * histLength];
 		List<Double> binsA = new ArrayList<Double>();
 		List<Double> binsB = new ArrayList<Double>();
 		for (int i = 0; i < histLength; i++) {
 			for (int j = 0; j < dimension; j++) {
 				binsA.add(bins[i * dimension + j]);
 			}
 			for (int j = 0; j < histLength; j++) {
 				for (int k = 0; k < dimension; k++) {
 					binsB.add(bins[j * dimension + k]);
 				}
 				costMatrix[i * histLength + j] = DistanceUtil.getGroundDist(binsA, binsB, DistanceType.LTWO, null);
 				binsB.clear();
 			}
 			binsA.clear();
 		}
 		return costMatrix;
	}
}
