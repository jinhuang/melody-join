package com.iojin.melody.utils;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.NonNegativeConstraint;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

public class DualBound {
	
	private static final double epsilon = 1.0e-4;
	private static final int maxUlps = 10;

	private static SimplexSolver solver = new SimplexSolver(epsilon, maxUlps);
	private final static MaxIter maxIter = new MaxIter(500);
	private static PointValuePair optimal;
	
	private double[] solution;
	private double min;
	
	public DualBound(double[] histA, double[] histB, double[] bins, int dimension) {
		this.solution = getOptimalFeasibleSolution(histA, histB, bins, dimension);
		this.min = getMinSolution(this.solution);
	}
	
	public DualBound(double[] solution) {
		this.solution = solution;
		this.min = getMinSolution(this.solution);
	}
	
	public DualBound(String value) {
		this(FormatUtil.toDoubleArray(value));
	}
	
	public double getMin() {
		return this.min;
	}
	
	public double getKey(double[] hist) {
		double key = 0.0;
		int num = solution.length / 2;
		for (int i = 0; i < num; i++) {
			key += hist[i] * solution[i];
		}
		return key;
	}
	
	public double getCKey(double[] hist) {
		double key = 0.0;
		int num = solution.length / 2;
		for (int i = 0; i < num; i++) {
			key += hist[i] * solution[i + num];
		}
		return key;		
	}
	
	public double[] getRange(double[] hist, double threshold) {
		double[] range = new double[2];
		range[0] = this.min + getKey(hist) - threshold;
		range[1] = threshold - getCKey(hist);
		return range;
	}
	
	public double getDualEmd(double[] histA, double[] histB) {
		double lowerA = getKey(histA) + getCKey(histB);
		double lowerB = getKey(histB) + getCKey(histA);
//		if (lowerA < 0 || lowerB < 0) {
//			System.out.println("A: key " + getKey(histA) + " ckey " + getCKey(histA));
//			System.out.println("B: key " + getKey(histB) + " ckey " + getCKey(histB));
//		}
		return lowerA < lowerB ? lowerB : lowerA;
	}
	
	@Override
	public String toString() {
		String text = FormatUtil.toTextString(solution);
		return text;
	}
	
	private double[] getOptimalFeasibleSolution(double[] histA, 
			double[] histB, double[] bins, int dimension) {
		int num = bins.length / dimension;
		Collection<LinearConstraint> constraints = new ArrayList<LinearConstraint>();
		double[] coefficients = new double[num * 2];
		for (int i = 0; i < num; i++) {
			for (int j = 0; j < num; j++) {
				coefficients = setIJ(coefficients, i, j);
				double dist = DistanceUtil.getGroundDist(DistanceUtil.getBin(bins, dimension, i),
						DistanceUtil.getBin(bins, dimension, j), DistanceType.LTWO, null);
				constraints.add(new LinearConstraint(coefficients, Relationship.LEQ, dist));
			}
		}
		
		for (int i = 0; i < num; i++) {
			coefficients[i] = histA[i];
			coefficients[i + num] = histB[i];
		}
		
		LinearObjectiveFunction objective = new LinearObjectiveFunction(coefficients, 0);
		LinearConstraintSet consSet = new LinearConstraintSet(constraints);
		NonNegativeConstraint nonNeg = new NonNegativeConstraint(false);
		optimal = solver.optimize(objective, consSet, nonNeg, GoalType.MAXIMIZE, maxIter);
		return optimal.getPoint();
	}
	
	private double[] setIJ(double[] coefficients, int i, int j) {
		int num = coefficients.length / 2;
		for (int each = 0; each < num; each++) {
			if (each == i) {
				coefficients[each] = 1.0;
			}
			else {
				coefficients[each] = 0.0;
			}
			if (each == j) {
				coefficients[each + num] = 1.0;
			}
			else {
				coefficients[each + num] = 0.0;
			}
		}
		return coefficients;
	}
	
	private double getMinSolution(double[] solution) {
		int num = solution.length / 2;
		double min = Double.MAX_VALUE;
		for (int i = 0; i < num; i++) {
			min = min > (solution[i] + solution[i + num]) ? (solution[i] + solution[i + num]) : min;
		}
		return min;
	}
}
