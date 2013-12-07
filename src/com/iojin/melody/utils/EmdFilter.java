package com.iojin.melody.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EmdFilter {
	private int dimension;
	private double[] bins;
	private int numBins;
	private List<Double[]> vectors;
	private int numVectors;
	private List<Double[]> projectedBins;
	private final int reducedDimension = 8;
	private final int numReductions = 4;
	private ReductionBound[] reductions = new ReductionBound[numReductions];
	private final int numDuals = 0;
	private DualBound[] duals = new DualBound[numDuals];
	private static Random r = new Random();
	
	public EmdFilter(int dimension, double[] bins, double[] vectors, List<Double[]> samples) {
		this.dimension = dimension;
		this.bins = bins;
		this.numBins = bins.length / dimension;
		this.numVectors = vectors.length / dimension;
		this.vectors = new ArrayList<Double[]>();
		for (int i = 0; i < this.numVectors; i++) {
			this.vectors.add(FormatUtil.toObjectDoubleArray(FormatUtil.getNthSubArray(vectors, dimension, i)));
		}
		this.projectedBins = new ArrayList<Double[]>();
		for (int i = 0; i < this.numVectors; i++) {
			this.projectedBins.add(FormatUtil.toObjectDoubleArray(HistUtil.projectBins(this.bins, dimension, FormatUtil.toDoubleArray(this.vectors.get(i)))));
		}
		
		for (int i = 0; i < numReductions; i++) {
			reductions[i] = new ReductionBound(dimension, reducedDimension, bins);
		}
		
		for (int i = 0; i < numDuals; i++) {
			int pickA = r.nextInt(samples.size());
			int pickB = r.nextInt(samples.size());
			while(pickA == pickB) r.nextInt(samples.size());
			double[] histA = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(samples.get(pickA)), 1, numBins));
			double[] histB = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(samples.get(pickB)), 1, numBins));
			duals[i] = new DualBound(histA, histB, bins, dimension);
		}
	}
	
	public boolean filter(double[] weightA, double[] weightB, double threshold) {
		// projection
		for (int i = 0; i < numVectors; i++) {
			if (HistUtil.getProjectEmd(weightA, weightB, FormatUtil.toDoubleArray(this.projectedBins.get(i))) > threshold) {
				return true;
			}
		}
		
		// rubner
		if (DistanceUtil.getRubnerEmd(weightA, weightB, dimension, bins, DistanceType.LTWO) > threshold) {
			return true;
		}
		
		// duals
		for (int i = 0; i < numDuals; i++) {
			if (duals[i].getDualEmd(weightA, weightB) > threshold) {
				return true;
			}
		}
		
		// reductions
		for (int i = 0; i < numReductions; i++) {
			if (reductions[i].getReducedEmd(weightA, weightB) > threshold) {
				return true;
			}
		}
		
		// indmin
		if (DistanceUtil.getIndMinEmd(weightA, weightB, dimension, bins, DistanceType.LTWO, null) > threshold) {
			return true;
		}
		
		return false;
	}
}
