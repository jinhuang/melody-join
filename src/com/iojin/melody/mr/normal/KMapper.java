package com.iojin.melody.mr.normal;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.iojin.melody.utils.FormatUtil;

public class KMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	@Override
	protected void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		double[] array = FormatUtil.toDoubleArray(val.toString());
		context.write(new LongWritable((long)array[0]), new Text(array[1] + " " + array[2] + " " + array[3]));
	}
}
