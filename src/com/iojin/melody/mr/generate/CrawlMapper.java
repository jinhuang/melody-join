package com.iojin.melody.mr.generate;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.iojin.melody.utils.ConfUtils;

public class CrawlMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	private static int numTask = 0;
	private static Random random;
	
	@Override
	protected void setup(Context context) {
		numTask = context.getConfiguration().getInt(ConfUtils.TASK, 48);
		random = new Random();
	}
	
	@Override
	protected void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		int id = random.nextInt() % numTask;
		context.write(new LongWritable(id), val);
	}
}
