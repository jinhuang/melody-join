package com.iojin.melody.mr.normal;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iojin.melody.utils.FormatUtil;

public class NEPreCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	private double minM = Double.MAX_VALUE;
	private double maxM = -Double.MAX_VALUE;
	private double minB = Double.MAX_VALUE;
	private double maxB = -Double.MAX_VALUE;
	
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
			double m = FormatUtil.getDouble(val, 1);
			double b = FormatUtil.getDouble(val, 2);
			minM = m < minM ? m : minM;
			maxM = m > maxM ? m : maxM;
			minB = b < minB ? b : minB;
			maxB = b > maxB ? b : maxB;
		}
		
		context.write(key, new Text(minM + " " + minB));
		context.write(key, new Text(maxM + " " + maxB));
	}
}
