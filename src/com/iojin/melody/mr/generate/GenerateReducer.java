package com.iojin.melody.mr.generate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iojin.melody.mr.EmdGenerate;
import com.iojin.melody.utils.ConfUtils;

public class GenerateReducer extends Reducer<Text, Text, Text, Text> {
	
	private String outputPath;
	private BufferedWriter writer;
	private FileSystem fs;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		outputPath = conf.get(ConfUtils.GENERATEOUTPUT);
		fs = FileSystem.get(conf);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException{
		String path = outputPath + "/" + EmdGenerate.HIST + "/" + key.toString();
		writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path), true)));
		for(Text value : vals) {
			writer.write(value.toString() + "\n");
		}
		writer.flush();
		writer.close();
	}
}
