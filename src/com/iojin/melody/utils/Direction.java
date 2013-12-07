package com.iojin.melody.utils;

public enum Direction {
	Northern,
	Northeastern,
	Eastern,
	Southeastern,
	Southern,
	Southwestern,
	Western,
	Northwestern,
	Inner;
	
	@Override
	public String toString() {
		String display;
		switch(this) {
		case Northern:
			display = "Northern";
			break;
		case Northeastern:
			display = "Northeastern";
			break;
		case Eastern:
			display = "Eastern";
			break;
		case Southeastern:
			display = "Southeastern";
			break;
		case Southern:
			display = "Southern";
			break;
		case Southwestern:
			display = "Southwestern";
			break;
		case Western:
			display = "Western";
			break;
		case Northwestern:
			display = "Northwestern";
			break;
		case Inner:
			display = "Inner";
			break;
		default:
			display = "";
			break;
		}
		return display;
	}
}
