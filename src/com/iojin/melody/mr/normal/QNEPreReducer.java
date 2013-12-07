package com.iojin.melody.mr.normal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.Grid;
import com.iojin.melody.utils.HistUtil;

public class QNEPreReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	
	private static int numVector = 0;
	private static double[] minM = new double[1];
	private static double[] maxM = new double[1];
	private static double[] minB = new double[1];
	private static double[] maxB = new double[1];
	private static int grid = 0;
	private static Logger logger = Logger.getLogger(QNEPreReducer.class);
	private static List<List<Double>> collectionM = new ArrayList<List<Double>>();
	private static List<List<Double>> collectionB = new ArrayList<List<Double>>();
	private static List<List<Double>> collectionSW = new ArrayList<List<Double>>();
	private static List<List<Double>> collectionSE = new ArrayList<List<Double>>();
	private static List<Double> percentile = new ArrayList<Double>();
	private static List<Double[]> quantileSW = new ArrayList<Double[]>();
	private static List<Double[]> quantileSE = new ArrayList<Double[]>();
	private static Percentile statistics = new Percentile();
	private static double[] t;
	private static Grid[] grids;
	private int dimension = 0;
	private int numBins = 0;
	private String binPath = "";
	private String vectorPath = "";
	private double[] bins;
	private double[] vector;
	private double[] projectedBins;
	
	@Override
	protected void setup(Context context) throws IOException {
		logger.setLevel(Level.WARN);
		numVector = context.getConfiguration().getInt("vector", 0);
		minM = new double[numVector];
		maxM = new double[numVector];
		minB = new double[numVector];
		maxB = new double[numVector];
		Configuration conf = context.getConfiguration();
		grid = conf.getInt("grid", 0);
		for (int i = 0; i < numVector; i++) {
			minM[i] = Double.MAX_VALUE;
			maxM[i] = -Double.MAX_VALUE;
			minB[i] = Double.MAX_VALUE;
			maxB[i] = -Double.MAX_VALUE;
			collectionM.add(new ArrayList<Double>());
			collectionB.add(new ArrayList<Double>());
			collectionSW.add(new ArrayList<Double>());
			collectionSE.add(new ArrayList<Double>());
			quantileSW.add(new Double[grid + 1]);
			quantileSE.add(new Double[grid + 1]);
		}
		double eachPercentile = 100.0 / grid;
		for (int i = 0; i < grid; i++) {
			percentile.add(i * eachPercentile + eachPercentile);
		}
		dimension = conf.getInt("dimension", 0);
		numVector = conf.getInt("vector", 0);
		numBins = conf.getInt("bins", 0);
		binPath = conf.get("binsPath");
		vectorPath = conf.get("vectorPath");
		String binsName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);	
		bins = FileUtil.getFromDistributedCache(conf, binsName, numBins * dimension);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, dimension * numVector);
		projectedBins = HistUtil.projectBins(bins, dimension, vector, numVector);	
		t = new double[2 * numVector];
		for (int i = 0; i < numVector; i++) {
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			t[i * 2] += HistUtil.getMinIn(eachProjection) - FormatUtil.avg(eachProjection);	
			t[i * 2 + 1] += HistUtil.getMaxIn(eachProjection) - FormatUtil.avg(eachProjection);			
		}
		grids = new Grid[numVector];
		
		// rank sample upper bound
		if (context.getConfiguration().get("query").equals("rank")) {
			System.out.println("Start computing global bound...");
			int paraK = context.getConfiguration().getInt("threshold", 0);
			String referencePath = context.getConfiguration().get("referencePath");
			List<Double[]> references = FileUtil.readMultiFromHDFS(context.getConfiguration(), referencePath);
			double globalBound = HistUtil.getKEmd(references, bins, dimension, paraK);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = new Path(context.getConfiguration().get("rankPath"));
			FSDataOutputStream out = fs.create(path);
			out.writeBytes(String.valueOf(globalBound));
			out.flush();
			out.close();
			System.out.println("Finished computing global bound " + globalBound);
		}
	}
	
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		 * find the minimum and maximum value of m and b
		 */
		for (Text val : values) {
			double[] domains = FormatUtil.getDoubleArray(val, 0, numVector * 3 - 1);
			for (int i = 0; i < numVector; i++) {
				double m = domains[i * 3 + 1];
				double b = domains[i * 3 + 2];
				minM[i] = m < minM[i] ? m : minM[i];
				maxM[i] = m > maxM[i] ? m : maxM[i];
				minB[i] = b < minB[i] ? b : minB[i];
				maxB[i] = b > maxB[i] ? b : maxB[i];
				
				// for quantile information
				collectionM.get(i).add(m);
				collectionB.get(i).add(b);
			}
		}
		
		// project points
		double [] slopes = new double[2 * numVector];
		for (int i = 0; i < numVector; i++) {
			slopes[i*2] = (-1) * t[i*2 + 1];
			slopes[i*2+1] = (-1) * t[i*2];
		}		
		double[] eachDomain = new double[4];
		double[] eachPoint = new double[2];
		for (int i = 0; i < numVector; i++) {
			eachDomain[0] = minM[i];
			eachDomain[1] = maxM[i];
			eachDomain[2] = minB[i];
			eachDomain[3] = maxB[i];
			double[] eachSlopes = FormatUtil.getNthSubArray(slopes, 2, i);
			logger.debug("To create a grid: domain " + 
					eachDomain[0] + ", " + eachDomain[1] + ", " + eachDomain[2] + ", " +
					eachDomain[3] + "; slopes " + eachSlopes[0] + ", " + eachSlopes[1] +
					"; numGrid " + grid);
			grids[i] = new Grid(eachDomain, eachSlopes, grid);
			
			for (int j = 0; j < collectionM.get(i).size(); j++) {
				eachPoint[0] = collectionM.get(i).get(j);
				eachPoint[1] = collectionB.get(i).get(j);
				double[] eachDistance = grids[i].getProjectionDistanceInGrid(eachPoint);
				collectionSW.get(i).add(eachDistance[0]);
				collectionSE.get(i).add(eachDistance[1]);
			}
		}
		
		// compute the quantile
		for (int i = 0; i < numVector; i++) {
			//Collections.sort(collectionSW.get(i));
			//Collections.sort(collectionSE.get(i));
			
			statistics.setData(FormatUtil.getArrayFromList(collectionSW.get(i)));
			quantileSW.get(i)[0] = statistics.evaluate(0.000001);
			for (int j = 1; j <= grid; j++) {
				if (j == grid) {
					quantileSW.get(i)[j] = statistics.evaluate(100);
				}
				else {
					quantileSW.get(i)[j] = statistics.evaluate(percentile.get(j-1));
				}
			}
			//quantileSW.set(i, FormatUtil.subtractMin(quantileSW.get(i)));
			
			statistics.setData(FormatUtil.getArrayFromList(collectionSE.get(i)));
			quantileSE.get(i)[0] = statistics.evaluate(0.000001);
			for (int j = 1; j <= grid; j++) {
				if (j == grid) {
					quantileSE.get(i)[j] = statistics.evaluate(100);
				}
				else {
					quantileSE.get(i)[j] = statistics.evaluate(percentile.get(j-1));
				}
			}
			//quantileSE.set(i, FormatUtil.subtractMin(quantileSE.get(i)));
		}
		
		/*
		 * write out record (i, minM maxM minB maxB)
		 * multiple lines
		 * no need to print out these since quantiles are printed
		 */
//		for (int i = 0; i < numVector; i++) {
//			context.write(new LongWritable(i), new Text(minM[i] + " " + maxM[i] + " " + minB[i] + " " + maxB[i]));
//		}
		
		/*
		 * write out quantile record 
		 * (i, minM maxM minB maxB sw1 ... sw(grid) se1 ... se(grid))
		 * multiple lines
		 */
		for (int i = 0; i < numVector; i++) {
			String eachRecord = minM[i] + " " + maxM[i] + " " + minB[i] + " " + maxB[i];
			//for (int j = 0; j < (grid - 1); j++) {
			for (int j = 0; j < grid + 1; j++) {
				eachRecord += " " + quantileSW.get(i)[j];
			}
			//for (int j = 0; j < (grid -1); j++) {
			for (int j = 0; j < grid + 1; j++) {
				eachRecord += " " + quantileSE.get(i)[j];
			}
			context.write(new LongWritable(i), new Text(eachRecord));
		}
		
	}
}
