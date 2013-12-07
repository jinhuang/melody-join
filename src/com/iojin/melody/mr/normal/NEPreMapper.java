package com.iojin.melody.mr.normal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.iojin.melody.utils.DualBound;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.HistUtil;

public class NEPreMapper extends Mapper<Object, Text, LongWritable, Text> {
	
	private int dimension = 0;
	private int numBins = 0;
	private int numVector = 0;
	private String binPath = "";
	private String vectorPath = "";
	private String dualPath = "";
	private String referencePath = "";
	private double[] bins;
	private double[] vector;
	private double[] projectedBins;
	private Logger logger;
	
	private static String queryType;
	
	/* histograms for computing duals */
	private static double[] histA;
	private static double[] histB;
	
	/* for sampling k upper bound */
	private List<Double[]> samples;
	private static int sampleRatio = 25;
	private static Random random = new Random();
	
	@Override
	protected void setup(Context context) throws IOException {
		logger = Logger.getLogger(getClass());
		logger.setLevel(Level.WARN);
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("dimension", 0);
		numVector = conf.getInt("vector", 0);
		numBins = conf.getInt("bins", 0);
		binPath = conf.get("binsPath");
		vectorPath = conf.get("vectorPath");
		dualPath = conf.get("dualPath");
		String binsName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);	
		bins = FileUtil.getFromDistributedCache(conf, binsName, numBins * dimension);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, dimension * numVector);
		projectedBins = HistUtil.projectBins(bins, dimension, vector, numVector);
		
		System.out.println("dual path : " + dualPath);
		
		queryType = conf.get("query");
		if (queryType.equals("rank") || queryType.equals("knn")) {
			referencePath = conf.get("referencePath");
		}
		
		samples = new ArrayList<Double[]>();
	}
	
	@Override
	protected void map (Object key, Text values, Context context) 
			throws IOException, InterruptedException {
		
		/*
		 * get weights in bins from the second value (the first is the id)
		 */
		double [] weights = FormatUtil.getNormalizedDoubleArray(values, 1, numBins);
		//logger.debug("weights: " + FormatUtil.toString(weights));
		
		/*
		 * approximate the projected histogram using a normal distribution
		 */
		NormalDistributionImpl[] normals = new NormalDistributionImpl[numVector];
		for (int i = 0; i < numVector; i++) {
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			eachProjection = FormatUtil.substractAvg(eachProjection);
			normals[i] = HistUtil.getNormal(weights, eachProjection);
		}
		
		/*
		 * transform to Hough space
		 */
		double[] ms = new double[numVector];
		double[] bs = new double[numVector];
		for (int i = 0; i < numVector; i++) {
			ms[i] = 1 / normals[i].getStandardDeviation();
			bs[i] = (-1) * normals[i].getMean() / normals[i].getStandardDeviation();
		}
		
		/*
		 * write (0, i m b ... i m b) record
		 */
		String record = "";
		for (int i = 0; i < numVector; i++) {
			record += i + " " + String.valueOf(ms[i]) + " " + String.valueOf(bs[i]) + " ";
		}
		record = record.trim();
		context.write(new LongWritable(0), new Text(record));
		
		if (null == histA) {
			histA = weights;
		}
		else if (null == histB) {
			histB = weights;
		}
		
		if (random.nextInt() % sampleRatio == 0) {
			samples.add(FormatUtil.toObjectDoubleArray(weights));
		}
		
		System.out.println(HistUtil.getMaxFlow(weights, bins, dimension));
	}

	
	@Override
	protected void cleanup(Context context) throws IOException {
		
		/* write out the dual computed on hdfs, in the path of dualPath 
		 * each dual file contains only ONE dual and has a random string name*/
		FileSystem fs = FileSystem.get(context.getConfiguration());
		String randomName = Long.toHexString(Double.doubleToLongBits(Math.random()));
		Path path = new Path(dualPath + File.separator + randomName);
		FSDataOutputStream out = fs.create(path);
 		
		DualBound dual = new DualBound(histA, histB, bins, dimension);
		out.writeBytes(dual.toString());
		out.flush();
		out.close();
		
		if (((long) Math.random()) % 2 == 0 && (queryType.equals("rank") || queryType.equals("knn"))) {
			path = new Path(referencePath + File.separator + randomName);
			out = fs.create(path);
//			out.writeBytes(FormatUtil.formatDoubleArray(histA));
//			out.writeBytes("\n");
//			out.writeBytes(FormatUtil.formatDoubleArray(histB));
			for (Double[] sample : samples) {
				out.writeBytes(FormatUtil.formatDoubleArray(FormatUtil.toDoubleArray(sample)));
				out.writeBytes("\n");
			}
			out.flush();
			out.close();
		}
	}

}