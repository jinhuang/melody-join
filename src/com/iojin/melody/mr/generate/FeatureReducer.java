package com.iojin.melody.mr.generate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iojin.melody.utils.ConfUtils;

public class FeatureReducer extends Reducer<Text, Text, LongWritable, Text>{
	
	private static String outputPath;
	private static String histPath;
	private static BufferedWriter writer;
	private static FileSystem fs;
	
	@Override
	protected void setup(Context context) throws IOException {
		outputPath = context.getConfiguration().get(ConfUtils.GENERATEOUTPUT);
		histPath = outputPath + "/" + "hist";
		fs = FileSystem.get(context.getConfiguration());
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException {
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(histPath + "/" + key.toString()), true)));
		for (Text val : vals) {
			writer.write(val.toString() + "\n");
		}
		writer.close();
	}
}
