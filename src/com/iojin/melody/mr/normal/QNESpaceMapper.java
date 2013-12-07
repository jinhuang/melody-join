package com.iojin.melody.mr.normal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;


import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.DualBound;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.Grid;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.QuantileGrid;

public class QNESpaceMapper extends Mapper<Object, Text, Text, Text> {
	private static int grid = 0;
	private static int numInterval = 0;
	private static int dimension = 0;
	private static int numBins = 0;
	private static int numVector = 0;
	private static String binPath = "";
	private static String vectorPath = "";
	//private static String tPath = "";
	private static String referencePath = "";
	private static double[] bins;
	private static double[] vector;
	private static double[] projectedBins;
	private static double[] t;
	private static double[] quantileArray;
	private static double[] quantile;
	private static Grid[] grids;
	
	
	private static Logger logger =  Logger.getLogger(QNESpaceMapper.class);
	
	/* for computing keys and ckeys on histograms */
	private static DualBound[] duals;
	
	private static String queryType;
	private static List<Double[]> references;
	
	
	@Override
	protected void setup(Context context) throws IOException {
		logger.setLevel(Level.WARN);
		Configuration conf = context.getConfiguration();
		grid = conf.getInt("grid", 0);
		numInterval = conf.getInt("interval", 5);
		dimension = conf.getInt("dimension", 0);
		numBins = conf.getInt("bins", 0);
		numVector = conf.getInt("vector", 0);
		binPath = conf.get("binsPath");
		vectorPath = conf.get("vectorPath");
		//tPath = conf.get("tPath");
		String binsName = FileUtil.getNameFromPath(binPath);
		String vectorName = FileUtil.getNameFromPath(vectorPath);
		//String tName = FileUtil.getNameFromPath(tPath);
		String domainName = "domain";
		bins = FileUtil.getFromDistributedCache(conf, binsName, numBins * dimension);
		vector = FileUtil.getFromDistributedCache(conf, vectorName, numVector * dimension);
		projectedBins = HistUtil.projectBins(bins, dimension, vector, numVector);
//		t = FileUtil.getFromDistributedCache(conf, tName, numVector * 2);
		t = new double[2 * numVector];
		for (int i = 0; i < numVector; i++) {
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			t[i * 2] += HistUtil.getMinIn(eachProjection) - FormatUtil.avg(eachProjection);	
			t[i * 2 + 1] += HistUtil.getMaxIn(eachProjection) - FormatUtil.avg(eachProjection);			
		}		
		
		quantileArray = FileUtil.getMultiFromDistributedCache(conf, domainName, 1 + (grid + 3) * 2);
		logger.debug("Read quantile: " + FormatUtil.toString(quantileArray));
		quantile = FormatUtil.omitVectorId(quantileArray, 1 + (grid + 3) * 2);
		logger.debug("Converted quantile: " + FormatUtil.toString(quantile));
		double [] slopes = new double[2 * numVector];
		logger.debug("Read t: " + FormatUtil.toString(t));
		for (int i = 0; i < numVector; i++) {
			slopes[i*2] = (-1) * t[i*2 + 1];
			slopes[i*2+1] = (-1) * t[i*2];
		}
		logger.debug("Convered slopes: " + FormatUtil.toString(slopes)); 

		grids = new QuantileGrid[numVector];
		for (int i = 0; i < numVector; i++) {
			double[] each = FormatUtil.getNthSubArray(quantile, 4 + 2*(grid+1), i);
			double[] eachDomain = FormatUtil.getSubArray(each, 0, 3);
			double[] eachSlopes = FormatUtil.getNthSubArray(slopes, 2, i);
			double[] eachSW = FormatUtil.getSubArray(each, 4, 3 + (grid+1));
			double[] eachSE = FormatUtil.getSubArray(each, 4 + grid + 1, 3 + 2 * (grid+1));
			grids[i] = new QuantileGrid(eachDomain, eachSlopes, grid, eachSW, eachSE);
		}
		
		/*
		 * multiple dual files, the read result will be a list of duals
		 */
		List<Double[]> dualSolutions = FileUtil.readMultiFromHDFS(conf, conf.get("dualPath"));
		duals = new DualBound[dualSolutions.size()];
		int counter = 0;
		for(Double[] solution : dualSolutions) {
			String solutionString = FormatUtil.toTextString(solution);
			duals[counter] = new DualBound(solutionString);
			counter++;
		}
		
		queryType = conf.get("query");
		if (queryType.equals("rank") || queryType.equals("knn")) {
			referencePath = conf.get("referencePath");
			references = FileUtil.readMultiFromHDFS(conf, referencePath);
		}
	}
	
	@Override
	protected void map (Object key, Text values, Context context) 
			throws IOException, InterruptedException {
		/*
		 * get weights in bins from the second value (the first is the id)
		 */
		double [] weights = FormatUtil.getNormalizedDoubleArray(values, 1, numBins);
		

		NormalDistributionImpl[] normals = new NormalDistributionImpl[numVector];
		List<TreeMap<Double, Double>> cdfs = new ArrayList<TreeMap<Double, Double>>(numVector);	
		int eachErrorLength = numInterval * 2 + 1;
		double [] error = new double[eachErrorLength * numVector];	
		double[] ms = new double[numVector];
		double[] bs = new double[numVector];
		long [] gridIds = new long[numVector];
		for (int i = 0; i < numVector; i++) {		
			
			/*
			 * approximate the projected histogram using a normal distribution
			 */
			double[] eachProjection = FormatUtil.getNthSubArray(projectedBins, numBins, i);
			eachProjection = FormatUtil.substractAvg(eachProjection);

			normals[i] = HistUtil.getNormal(weights, eachProjection);		
			
			/*
			 * get discrete CDF
			 */
			TreeMap<Double, Double> cdf = HistUtil.getDiscreteCDFNormalized(weights, eachProjection);
//			TreeMap<Double, Double> cdf = HistUtil.getDiscreteCDF(weights, eachProjection);
			cdfs.add(cdf);
			
			/*
			 * get approximation errors of normal distribution towards discrete cdf
			 */
			List<Double> errors = HistUtil.getMinMaxError(normals[i], cdfs.get(i), numInterval);
			double fullError = HistUtil.getFullError(normals[i], cdfs.get(i), t[i*2], t[i*2+1]);
			for (int j = 0; j < errors.size(); j++) {
				error[i * eachErrorLength + j] = errors.get(j);
			}
			error[i * eachErrorLength + errors.size()] = fullError;	
//			if ( i == 0 && error[i * eachErrorLength] > 100000000000L) {
//				logger.debug("Normal " + normals[0].getMean() + " : " + normals[0].getStandardDeviation());
//				logger.debug("CDF: " + FormatUtil.toString(cdfs.get(i)));
//			}
			
			/*
			 * transform to Hough space
			 */	
			ms[i] = 1 / normals[i].getStandardDeviation();
			bs[i] = (-1) * normals[i].getMean() / normals[i].getStandardDeviation();
			
			/*
			 * locate the record in a grid in the transformed space
			 */			
			double[] record = new double[2];
			record[0] = ms[i];
			record[1] = bs[i];
			gridIds[i] = grids[i].getGridId(record);
			if (gridIds[i] < 0) {
				gridIds[i] = 0;
			}
			
		}
		
		// debugging
//		for (int i = 0; i < numVector; i++) {
//			if (gridIds[i] > grid * grid) {
//				logger.debug("weird grid id: " + gridIds[i]);
//			}
//		}
		
		// dual keys
		double[] keys = new double[duals.length];
		for (int i = 0; i < duals.length; i++) {
			keys[i] = duals[i].getKey(weights);
		}
		
		// rubner keys
		double[] rubnerKeys = DistanceUtil.getRubnerValue(weights, dimension, bins);
		
		/*
		 * use gridId combination as the key
		 * write record (gridId, minError1 maxError1 minError2 maxError2 ... fullError) dual keys, rubner keys
		 */			
		String combination = FormatUtil.formatCombination(gridIds);
		String outVal = FormatUtil.formatDoubleArray(error) + " " + FormatUtil.formatDoubleArray(keys) + " " + FormatUtil.formatDoubleArray(rubnerKeys);
		
		if (queryType.equals("rank") || queryType.equals("knn")) {
			outVal += ";";
			for (Double[] ref : references) {
				double flow = HistUtil.getFlowBetween(weights, ref, bins, dimension);
				outVal += " " + flow;
			}
		}
		
		
		context.write(new Text(combination), new Text(outVal));		

	}
}
