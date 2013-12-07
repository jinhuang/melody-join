package com.iojin.melody.utils;

import java.text.DecimalFormat;

public class TimerUtil {
	private static long timer = 0;
	private static DecimalFormat formatter = new DecimalFormat("#.##");
	public static long intersectionTimer = 0;
	public static long noIntersectionTimer = 0;
	public static long intersectionCounter = 0;
	public static long noIntersectionCounter = 0;
	public static long emdbrCounter = 0;
	public static long eliminatedCounter = 0;
	public static long dualElimination = 0;
	public static long rankEliminatedCounter = 0;
	public static long rankTimer = 0;
	public static long knnTimer = 0;
	public static long rankBoundTimer = 0;
	public static long rankPruneTimer = 0;
	public static long rankEliminationTimer = 0;
	public static long knnBoundTimer = 0;
	public static long knnPruneTimer = 0;
	public static long knnEliminationTimer = 0;
	
	public static long qnePairCounter = 0;
	public static long bflbPairCounter = 0;
	
	public static void start() {
		timer = System.nanoTime();
	}
	
	public static void end() {
		if (timer == 0) {
			timer = -1;
		}
		else {
			timer = System.nanoTime() - timer;
		}
	}
	
	public static long getRaw() {
		return timer;
	}
	
	public static long getMili() {
		return timer / 1000000;
	}
	
	public static double getSecond() {
		return format((double) getMili() / 1000.0);
	}
	
	public static double getMinute() {
		return format(getSecond() / 60.0);
	}
	
	public static double getHour() {
		return format(getMinute() / 60.0);
	}
	
	public static void print() {
		String display = "Wall clock elasped time: ";
		if (getHour() < 1.0) {
			if (getMinute() < 1.0) {
				if (getSecond() < 1.0) {
					if (getMili() < 1.0) {
						display += format(getRaw()) + " nanoseconds";
					}
					else {
						display += format(getMili()) + " miliseconds";
					}
				}
				else {
					display += format(getSecond()) + " seconds";
				}
			}
			else {
				display += format(getMinute()) + " minutes";
			}
		}
		else {
			display += format(getHour()) + " hours";
		}
		System.out.println(display);
	}
	
	private static Double format(double val) {
		return Double.parseDouble(formatter.format(val));
	}
}
