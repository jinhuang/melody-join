package com.iojin.melody.utils;

public class BinDistance implements Comparable<BinDistance> {
	private double dist;
	private int index;

	public BinDistance(double dist, int index) {
		this.dist = dist;
		this.index = index;
	}
	
	public double getDist() {
		return dist;
	}

	public void setDist(double dist) {
		this.dist = dist;
	}

	public int getIndex() {
		return this.index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}



	@Override
	public int compareTo(BinDistance o) {
		if (Math.abs(this.dist - o.dist) < DistanceUtil.EPSILON) return 0;
		else return (this.dist - o.dist) > 0 ? 1 : -1;
	}
	
	@Override
	public String toString() {
		return "[" + this.dist + " : " + this.index  + "]";
	}
}
