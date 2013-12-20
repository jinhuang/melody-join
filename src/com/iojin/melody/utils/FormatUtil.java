package com.iojin.melody.utils;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

public class FormatUtil {
	
	private static final String delimiter = "\\s";
	private static final DecimalFormat df = new DecimalFormat("#.####");
	private static final double epsilon = 0.000000000001;
	
	private static String[] tokenize(String value) {
		return value.split(delimiter);
	}

	private static double nice(Double val) {
		return nice(val.doubleValue());
	}
	
	private static double nice(double val) {
		return Double.valueOf(df.format(val));
	}
	
	public static long getLong(String text, int offset) {
		return Long.valueOf(tokenize(text)[offset]);
	}

	public static double getDouble(String text, int offset) {
		return nice(Double.valueOf(tokenize(text)[offset]));
	}
	
	public static long getLong(Text text, int offset) {
		return Long.valueOf(tokenize(text.toString())[offset]);
	}
	
	public static double getDouble(Text text, int offset) {
		return nice(Double.valueOf(tokenize(text.toString())[offset]));
	}
	
	public static double[] getNormalizedDoubleArray(Text text, int start, int end) {
		double[] array = getDoubleArray(text, start, end);
		return HistUtil.normalizeArray(array);
	}
	
	public static double[] getDoubleArray(Text text, int start, int end) {
		double[] record = new double [end - start + 1];
		for (int i = 0; i < record.length; i++) {
			record[i] = Double.valueOf(tokenize(text.toString())[start + i]);
		}
		return record;
	}
	
	public static double[] getDoubleArray(String text, int start, int end) {
		double[] record = new double [end - start + 1];
		for (int i = 0; i < record.length; i++) {
			record[i] = Double.valueOf(tokenize(text)[start + i]);
		}
		return record;		
	}
	
	public static Double[] getDoubleObjectArray(String text, int start, int end) {
		Double[] record = new Double [end - start + 1];
		for (int i = 0; i < record.length; i++) {
			record[i] = Double.valueOf(tokenize(text)[start + i]);
		}
		return record;		
	}	
	
	public static double[] getSubArray(double[] array, int start, int end) {
		double[] sub = new double[end - start + 1];
		for (int i = 0; i <= (end - start); i++) {
			sub[i] = array[i + start];
		}
		return sub;
	}
	public static Double[] getSubArray(Double[] array, int start, int end) {
		Double[] sub = new Double[end - start + 1];
		for (int i = 0; i <= (end - start); i++) {
			sub[i] = array[i + start];
		}
		return sub;
	}	
	
	
	public static double[] getSubArray(double[] array, int start, int end, double[] sub) {

		for (int i = 0; i <= (end - start); i++) {
			sub[i] = array[i + start];
		}
		return sub;
	}	
	
	public static String formatDoubleArray(double[] values) {
		String out = "";
		for (double val : values) {
			out += val + " ";
		}
		return out.trim();
	}
	
	public static String formatDoubleArray(List<Double> values) {
		String out = "";
		for (double val : values) {
			out += val + " ";
		}
		return out.trim();
	}
	
	public static double[] getCountFromError(double[] error, int size) {
		double[] count = new double[error.length / size * 2];
		for (int i = 0; i < count.length; i++) {
			if (0 == i % 2) {
				count[i] = error[i*size];
			}
			else {
				count[i] = error[i*size + 1];
			}
		}
		return count;
	}
	
	public static double[] listToArray(List<Double> list) {
		double[] array = new double[list.size()];
		for (int i = 0; i < array.length; i++) {
			array[i] = list.get(i);
		}
		return array;
	}
	
	// notice the vectorId should be continuous, i.e., 0, 1, 2, 3, 4, ...
	public static double[] omitVectorId(double[] values, int size) {
		int num = values.length / size;
		double[] result = new double[num * (size - 1)];
		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();		
		for (int i = 0; i < num; i++) {
			map.put((int) values[i * size], i);
		}
		for (int i = 0; i < num; i++) {
			for (int j = 0; j < (size-1); j++) {
				result[i * (size-1) + j] = values[map.get(i) * size + j + 1];
			}
		}		
		return result;
	}
	
	public static double[] getNthSubArray(double[] values, int size, int n) {
		return getSubArray(values, n * size, n * size + size - 1);
	}
	
	public static double[] getNthSubArray(double[] values, int size, int n, double[] sub) {
		return getSubArray(values, n * size, n * size + size - 1, sub);
	}	
	
	public static String formatCombination(long[] ids) {
		String idString = "";
		for (long id : ids) {
			idString += id + " ";
		}
		return idString.trim();
	}
	
	public static long[] parseCombination(String idString) {
		String[] idsArray = idString.split("\\s");
		long[] ids = new long[idsArray.length];
		for (int i = 0; i < ids.length; i++) {
			ids[i] = Long.valueOf(idsArray[i]);
		}
		return ids;
	}
	
	
	
	public static String toString(double[] array) {
		String display = "[";
		for (double val : array) {
			display += val + ", ";
		}
		display = display.substring(0, display.length() - 2);
		display +="]";
		return display;
	}
	
	public static String toTextString(int[] array) {
		String string = "";
		for (int val : array) {
			string += val + " ";
		}
		string.trim();
		return string;
	}
	
	public static String toTextString(double[] array) {
		String string = "";
		for (double val : array) {
			string += val + " ";
		}
		string.trim();
		return string;
	}
	
	public static String toTextString(Double[] array) {
		String string = "";
		for (Double val : array) {
			string += val + " ";
		}
		string.trim();
		return string;
	}	
	
	public static String toString(List<String> list) {
		String display = "[";
		for (String val : list) {
			display += val + ", ";
		}
		if (display.length() > 2) {
			display = display.substring(0, display.length() - 2) + "]";
		}
		else {
			display += "]";
		}		
		
		return display;
	}
	
	public static String toString(TreeMap<Double, Double> map) {
		String display = "[";
		for (Map.Entry<Double, Double> entry : map.entrySet()) {
			display += nice(entry.getKey()) + " : " + nice(entry.getValue()) + ", ";
		}
		display = display.substring(0, display.length() - 2) + "]";
		return display;
	}
	
	public static String toString(HashMap<String, Long> map) {
		String display = "[";
		for (Map.Entry<String, Long> entry : map.entrySet()) {
			display += entry.getKey() + " : " + entry.getValue() + ", ";
		}
		display = display.substring(0, display.length() - 2) + "]";
		return display;
	}
	
	public static String setToString(HashMap<String, Set<String>> map){
		String display = "[";
		for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
			display += entry.getKey() + " : ";
			for (String text : entry.getValue()) {
				display += text + ", ";
			}
			if (display.contains(",")) {
				display = display.substring(0, display.length() - 2);
			}
			display += "; ";
		}
		if (display.contains(";")) {
			display = display.substring(0, display.length() - 2);
		}
		return display + "]";
	}
	
	public static String toStringLong(HashMap<Long, Long> map) {
		String display = "[";
		for (Map.Entry<Long, Long> entry : map.entrySet()) {
			display += entry.getKey() + " : " + entry.getValue() + ", ";
		}
		display = display.substring(0, display.length() - 2) + "]";
		return display;
	}	
	
	public static String getCombination(String val, int numVector) {
		String[] values = val.split("\\s");
		String combination = "";
		for (int i = 0; i < numVector; i++) {
			combination += values[i] + " ";
		}
		return combination.trim();
	}
	
	public static int countZero(double[] array) {
		int count = 0;
		for (double val : array) {
			if ((val - 0) < epsilon) {
				count++;
			}
		}
		return count;
	}
	
	public static double[] toDoubleArray(String value) {
		String[] values = value.split("\\s");
		double[] array = new double[values.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = values[i].trim();
			if (values[i].length() > 0) {
				array[i] = Double.valueOf(values[i]);
			}
		}
		return array;
	}
	
	public static Double[] toObjectDoubleArray(String value) {
		String[] values = value.split("\\s");
		Double[] array = new Double[values.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = values[i].trim();
			if (values[i].length() > 0) {
				array[i] = Double.valueOf(values[i]);
			}
		}
		return array;		
	}
	
	public static Double[] toObjectDoubleArray(double[] array) {
		Double[] result = new Double[array.length];
		for (int i = 0; i < array.length;i++) {
			result[i] = Double.valueOf(array[i]);
		}
		return result;
	}
	
	public static double[] toDoubleArray(Double[] array) {
		double[] result = new double[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = array[i].doubleValue();
		}
		return result;
	}
	
	public static void main(String[] args) {
		double[] test = {1, 3, 4, 5, 2, 0, 2, 7, 3, 1, 3, 2,3 ,3,4, 2, 1,2 ,3,1};
		System.out.println(countZero(test));
		System.exit(0);
	}
	
	public static double[] getArrayFromList(List<Double> list) {
		double[] array = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = list.get(i).doubleValue();
		}
		return array;
	}
	
	public static double[] listToOneArray(List<Double[]> list) {
		if (list != null && list.size() > 0) {
			int each = list.get(0).length;
			double[] array = new double[list.size() * each];
			for (int i = 0; i < list.size(); i++) {
				for (int j = 0; j < each; j++) {
					array[i*each + j] = list.get(i)[j];
				}
			}
			return array;
		}
		return null;
	}
	
	public static Double[] subtractMin(Double[] array) {
		for (int i = 0; i < array.length; i++) {
			array[i] = array[i] - array[0];
		}
		return array;
	}
}
