package com.iojin.melody.utils;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class QuantileGrid extends Grid {
	
	private Logger logger;
	
	// besides the number of grid per side, there must be size of intercept
	// in cumulative form
	private double[] interceptSE;
	private double[] interceptSW;
	
	/*
	 * Constructor
	 */
	public QuantileGrid(double[] inDomain, double[] inSlope, 
			int inSideNum, double[] tMinInt, double[] tMaxInt) {
		super(inDomain, inSlope, inSideNum);
		interceptSE = tMaxInt;
		interceptSW = tMinInt;
		logger = Logger.getLogger(getClass());
		logger.setLevel(Level.DEBUG);	
	}
	
	/*
	 * (non-Javadoc)
	 * @see nate.util.Grid#countOffset(double, nate.util.Direction)
	 */
	@Override
	protected int countOffset(double offset, Direction direction) {
		int count = -1;
		double[] intercept;
		switch(direction) {
			case Southwestern:
				intercept = interceptSW;
				break;
			case Southeastern:
				intercept = interceptSE;
				break;
			default:
				return -1;
		}
		count = Arrays.binarySearch(intercept, offset);
		if (count < 0) {
			count = (-1) * (count + 1) - 1;
		}
		if (count < 0) {
			count = 0;
		}
		return count;
	}
	
	@Override
	protected double[] getIntersectionByCount(int countSW, int countSE) {
		double[] pointSW = new double[2];
		double[] pointSE = new double[2];
		pointSW[0] = pointSouthern[0] - getX(interceptSW[countSW], slopes[0]);
		pointSW[1] = pointSouthern[1] + getY(interceptSW[countSW], slopes[0]);
		double[] lineSE = getLine(pointSW, slopes[1]);
		pointSE[0] = pointSouthern[0] + getX(interceptSE[countSE], slopes[1]);
		pointSE[1] = pointSouthern[1] + getY(interceptSE[countSE], slopes[1]);
		double[] lineSW = getLine(pointSE, slopes[0]);
		double[] intersection = getIntersection(lineSE, lineSW);
		return intersection;
	}
	
}
