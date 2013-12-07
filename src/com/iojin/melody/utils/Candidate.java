package com.iojin.melody.utils;

public class Candidate implements Comparable<Candidate>{
	private long rid;
	private double[] weights;
	private String nativeGrids;
	private String combination;
	private int count;
	private double upper;
	private double lower;
	
	public Candidate(String combination, int count, double upper, double lower) {
		this.combination = combination;
		this.count = count;
		this.upper = upper;
		this.lower = lower;
	}
	
	public String getCombination() {
		return combination;
	}
	public void setCombination(String combination) {
		this.combination = combination;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public double getUpper() {
		return upper;
	}
	public void setUpper(double upper) {
		this.upper = upper;
	}
	public double getLower() {
		return lower;
	}
	public void setLower(double lower) {
		this.lower = lower;
	}
	
	public long getRid() {
		return this.rid;
	}
	
	public void setRid(long rid) {
		this.rid = rid;
	}
	
	public double[] getWeights() {
		return this.weights;
	}
	
	public void setWeights(double[] weights) {
		this.weights = weights;
	}
	
	public String getNativeGrids() {
		return this.nativeGrids;
	}
	
	public void setNativeGrids(String nativeGrids) {
		this.nativeGrids = nativeGrids;
	}

	@Override
	public int compareTo(Candidate o) {
		if (Math.abs(this.upper - o.upper) < DistanceUtil.EPSILON) {
			int diff = this.combination.compareTo(o.combination);
			return diff == 0 ? (int) (this.rid - o.rid) : diff;
		}
		else return (this.upper - o.upper) > 0 ? 1 : -1;
	}
	
	
}
