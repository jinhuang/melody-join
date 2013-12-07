package com.iojin.melody.utils;

public class Bin implements Comparable<Bin>{
	private double weight;
	private int index;
	
	public Bin(double weight, int index) {
		this.weight = weight;
		this.index = index;
	}
	
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	
	@Override
	public int compareTo(Bin o) {
		if (Math.abs(this.weight - o.weight) < DistanceUtil.EPSILON) {
			return (this.index - o.index);
		}
		else return (this.weight - o.weight) > 0 ? -1 : 1;
	}
	
	@Override
	public String toString() {
		return "[" + this.index + " : " + this.weight + "]";
	}
}
