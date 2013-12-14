package com.iojin.melody.mr.normal;

import java.io.File;
//import java.io.IOException;
import java.net.URI;

//import nate.util.FormatUtil;
//import nate.util.HistUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.TimerUtil;

public class QuantileNormalEmd extends Configured implements Tool {

	private static final int gridIt = 0;
	private static final int intervalIt = 1;
	private static final int workerIt = 2;
	private static final int dimensionIt = 3;
	private static final int binsIt = 4;
	private static final int vectorNumIt = 5;
	private static final int thresholdIt = 6;
	private static final int queryIt = 7;
	private static final int ratioIt = 8;
	private static final int inputPathIt = 9;
	private static final int binsPathIt = 10;
	private static final int vectorPathIt = 11;
	private static final int outputPathIt = 12;
	private static final int argsLength = outputPathIt - gridIt + 1;
	
	private static final String cachePath = File.separator + "cache";
	private static final String preOutput = File.separator + "domain";
	private static final String t = File.separator + "minmax";
	private static final String error = File.separator + "error";
	private static final String dual = File.separator + "dual";
	private static final String key = File.separator + "key";
	private static final String rubner = File.separator + "rubner";
	private static final String reference = File.separator + "reference";
	private static final String rank = File.separator + "rank";
	private static final String finalResult = File.separator + "final";
	private static final String kResult = File.separator + "k";
	
	private static final int lightweightReducer = 1;
	
	public static int getArgLength() {
		return argsLength;
	}
	
	private static boolean parseArgs(String[] args, Configuration conf) {
		if (args.length != argsLength 
				|| null == Integer.valueOf(args[gridIt])
				|| null == Integer.valueOf(args[intervalIt])
				|| null == Integer.valueOf(args[workerIt])
				|| null == Double.valueOf(args[thresholdIt])
				|| null == Integer.valueOf(args[dimensionIt])
				|| null == Integer.valueOf(args[binsIt])
				|| null == Integer.valueOf(args[vectorNumIt])) {
			return false;
		}
		conf.set("grid", args[gridIt]);
		conf.set("interval", args[intervalIt]);
		conf.set("worker", args[workerIt]);
		conf.set("dimension", args[dimensionIt]);
		conf.set("bins", args[binsIt]);
		conf.set("vector", args[vectorNumIt]);
		conf.set("threshold", args[thresholdIt]);
		conf.set("paraK", args[thresholdIt]);
		conf.set("query", args[queryIt]);
		conf.set("ratio", args[ratioIt]);
		conf.set("inputPath", args[inputPathIt]);
		conf.set("binsPath", args[binsPathIt]);
		conf.set("vectorPath", args[vectorPathIt]);
		conf.set("outputPath", args[outputPathIt]);
		conf.set("domainPath", args[outputPathIt] + cachePath + preOutput);
		conf.set("tPath", args[outputPathIt] + cachePath + t);
		conf.set("errorPath", args[outputPathIt] + cachePath + error);
		conf.set("finalPath", args[outputPathIt] + finalResult);
		conf.set("kPath", args[outputPathIt] + kResult);
		conf.set("dualPath", args[outputPathIt] + cachePath + dual);
		conf.set("referencePath", args[outputPathIt] + cachePath + reference);
		conf.set("rankPath", args[outputPathIt] + cachePath + rank);
		conf.set("keyPath", args[outputPathIt] + cachePath + key);
		conf.set("rubnerPath", args[outputPathIt] + cachePath + rubner);
		printConfiguration(conf);
		return true;
	}
	
	private static void printConfiguration(Configuration conf) {	
		println("Configuration:");
		println("Grid: " + conf.getInt("grid", 0));
		println("Interval: " + conf.getInt("interval", 0));
		println("Worker: " + conf.getInt("worker", 0));
		println("Dimension: " + conf.getInt("dimension", 0));
		println("Bins: " + conf.getInt("bins", 0));
		println("Vector: " + conf.getInt("vector", 0));
		println("Threshold: " + conf.get("threshold"));
		println("Query: " + conf.get("query"));
		println("Sample ratio: " + conf.get("ratio"));
		println("Input path: " + conf.get("inputPath"));
		println("Bins path: " + conf.get("binsPath"));
		println("Vector path: " + conf.get("vectorPath"));
		println("Output path: " + conf.get("outputPath"));
		println("Intermediate Domain: " + conf.get("domainPath"));
		println("Intermediate t: " + conf.get("tPath"));
		println("Intermediate Error: " + conf.get("errorPath"));
		println("Final: " + conf.get("finalPath"));
	}
	
	private static void println(String content) {
		System.out.println(content);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		if (!QuantileNormalEmd.parseArgs(args, conf)) {
			return -1;
		}
		conf.set("mapred.child.java.opts", "-Xmx1024M");
		
		FileUtil.deleteIfExistOnHDFS(conf, conf.get("outputPath"));
		
		// Math3 dependency
		FileUtil.addDependency(conf);
		
		// ONE LINE bins, a set of d-dimensional locations, length = numBins * dimension
		// b0_1 b0_2 ...  b0_d b1_1 b1_2 ... b1_d ...
		DistributedCache.addCacheFile(new URI(conf.get("binsPath")),conf);
		
		// ONE LINE vectors, a set of d-dimensional vectors, length = numVector * dimension
		// v0_1 v0_2 ...  v0_d v1_1 v1_2 ... v1_d ...
		DistributedCache.addCacheFile(new URI(conf.get("vectorPath")),conf);
		
		// ONE LINE t (tMin, tMax) for each projection vector, length = numVector * 2
		// tMin1 tMax1 tMin2 tMax2 ...
		//projectBins(conf);
		//DistributedCache.addCacheFile(new URI(conf.get("tPath")), conf);
		
		TimerUtil.start();
		Job preJob=new Job(conf,"Quantile Normal EMD Join Phase 1 - Preprocessing");	
		preJob.setJarByClass(QuantileNormalEmd.class);
		preJob.setMapperClass(NEPreMapper.class);
		preJob.setMapOutputKeyClass(LongWritable.class);
		preJob.setMapOutputValueClass(Text.class);
		preJob.setReducerClass(QNEPreReducer.class);
		preJob.setOutputKeyClass(LongWritable.class);
		preJob.setOutputValueClass(Text.class);
		preJob.setNumReduceTasks(lightweightReducer);
		FileInputFormat.addInputPath(preJob, new Path(conf.get("inputPath")));
		String preOutputPath = conf.get("domainPath");
		FileOutputFormat.setOutputPath(preJob, new Path(preOutputPath));
		println("---- Starting " + preJob.getJobName() + " ----");
		preJob.waitForCompletion(true);
		println("---- Finished " + preJob.getJobName() + " ----");
		
		// MULTIPLE LINES domains (mean, variance) for each projection vector, line length = 5
		// vectorId minM1 maxM1 minB1 maxB1 
		// vectorId minM2 maxM2 minB2 maxB2 ...
		FileUtil.addOutputToDistributedCache(conf, preOutputPath, "domain");
		
		Job spaceJob = new Job(conf, "Quantile Normal EMD Join Phase 2 - Space");
		spaceJob.setJarByClass(QuantileNormalEmd.class);
		spaceJob.setMapperClass(QNESpaceMapper.class);
		spaceJob.setMapOutputKeyClass(Text.class);
		spaceJob.setMapOutputValueClass(Text.class);
		spaceJob.setReducerClass(NESpaceReducer.class);
		spaceJob.setOutputKeyClass(Text.class);
		spaceJob.setOutputValueClass(Text.class);
		spaceJob.setNumReduceTasks(conf.getInt("worker", 1)/2);
		FileInputFormat.addInputPath(spaceJob, new Path(conf.get("inputPath")));
		String spaceOutputPath = conf.get("errorPath");
		FileOutputFormat.setOutputPath(spaceJob, new Path(spaceOutputPath));
		println("---- Starting " + spaceJob.getJobName() + " ----");
		spaceJob.waitForCompletion(true);
		println("---- Finished " + spaceJob.getJobName() + " ----");
		
		// MULTIPLE LINES errors for each combinations, line length = numVector + 1 + (numInterval + 2) * 2
		// combination count eMin1 eMin2 ... eFullMin eMax1 eMax2 ... eFullMax 
		// combination count ...
//		DistributedCache.addCacheFile(new URI(conf.get("errorPath")), conf);
		FileUtil.addOutputToDistributedCache(conf, conf.get("errorPath"), "error");
		
		/*
		 * ONE file, multiple lines dual keys
		 * each line for one combination
		 */
		//DistributedCache.addCacheFile(new URI(conf.get("keyPath")), conf);		
		
		Job processJob = new Job(conf, "Quantile Normal EMD Join Phase 3 - Process");
		processJob.setJarByClass(QuantileNormalEmd.class);
		processJob.setMapperClass(QNEProcessMapper.class);
		processJob.setMapOutputKeyClass(LongWritable.class);
		processJob.setMapOutputValueClass(Text.class);
		String queryType = conf.get("query");
		if (queryType.equals("range")) {
			processJob.setReducerClass(NEProcessReducer.class);
			processJob.setOutputKeyClass(LongWritable.class);
			processJob.setOutputValueClass(LongWritable.class);
		}
		else if (queryType.equals("rank")) {
			processJob.setReducerClass(QNEProcessRankReducer.class);
			processJob.setOutputKeyClass(LongWritable.class);
			processJob.setOutputValueClass(Text.class);				
		}
		processJob.setNumReduceTasks(conf.getInt("worker", 1));
		FileInputFormat.addInputPath(processJob, new Path(conf.get("inputPath")));
		String processOutputPath = conf.get("finalPath");
		FileOutputFormat.setOutputPath(processJob, new Path(processOutputPath));
		println("---- Starting " + processJob.getJobName() + " ----");
		processJob.waitForCompletion(true);
		println("---- Finished " + processJob.getJobName() + " ----");		
		
		if (queryType.equals("knn") || queryType.equals("rank")) {
			Job lastJob = new Job(conf, "Quantile Normal Emd Join Phase 4 - Aggregate");
			lastJob.setJarByClass(QuantileNormalEmd.class);
			lastJob.setMapperClass(KMapper.class);
			lastJob.setMapOutputKeyClass(LongWritable.class);
			lastJob.setMapOutputValueClass(Text.class);
			lastJob.setReducerClass(KReducer.class);
			lastJob.setOutputKeyClass(LongWritable.class);
			lastJob.setOutputValueClass(Text.class);
			lastJob.setNumReduceTasks(1);
			FileInputFormat.addInputPath(lastJob, new Path(conf.get("finalPath")));
			FileOutputFormat.setOutputPath(lastJob, new Path(conf.get("kPath")));
			println("---- Starting " + lastJob.getJobName() + " ----");
			lastJob.waitForCompletion(true);
			println("---- Finished " + lastJob.getJobName() + " ----");			
		}
		
		TimerUtil.end();
		TimerUtil.print();
		return 0;

	}

}
