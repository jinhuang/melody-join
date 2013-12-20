package com.iojin.melody.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.imageanalysis.LireFeature;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.GenerateUtil;


public class EmdGenerate extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job();
		job.setJarByClass(EmdGenerate.class);
		job.setMapperClass(GenerateMapper.class);
		job.setReducerClass(GenerateReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Configuration conf = getConf();
		FileInputFormat.addInputPaths(job, conf.get(ConfUtils.GENERATEINPUT));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ConfUtils.GENERATEOUTPUT)));
		job.waitForCompletion(true);
		return 0;
	}
	
	private class GenerateMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
		private Set<String> featureNames;
		private Map<String, DocumentBuilder> builders;
		private Map<String, LireFeature> lireFeatures;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			featureNames = new HashSet<String>();
			String[] array = conf.get(ConfUtils.GENERATEFEATURE).split("/");
			for (String each : array) {
				featureNames.add(StringUtils.trim(each.toLowerCase()));
			}
			GenerateUtil.prepareFeaturesAndBuilders(featureNames, builders, lireFeatures);
			
		}
		
		@Override
		protected void map(LongWritable key, BytesWritable val, Context context) throws IOException, InterruptedException {
			// TODO
		}
	}
	
	private class GenerateReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException{
			// TODO
		}
	}
}
