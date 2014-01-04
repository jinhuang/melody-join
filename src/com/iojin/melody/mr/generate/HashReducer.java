package com.iojin.melody.mr.generate;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HashReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException{
		for (Text val : vals) {
			context.write(key, val);
		}
	}	
}
