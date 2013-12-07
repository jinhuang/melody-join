package com.iojin.melody.mr.normal;

import java.io.File;
import java.io.IOException;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;

public class NESpaceReducer extends Reducer<Text, Text, Text, Text> {

	private int numInterval = 0;
	private int numVector = 0;
	private int dimension = 0;
	private int numDuals = 0;
	
	private static Logger logger;
	private static double[] minErrors;
	private static double[] maxErrors;
	private static double[] minFullError;
	private static double[] maxFullError;
	
	private static double[] minKeys;
	private static double[] maxKeys;
	
	private static double[] minRubnerKeys;
	private static double[] maxRubnerKeys;
	
	private FSDataOutputStream os;
	private FileSystem fs;
	
	private static String queryType;
	private static double[] maxFlows;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		numInterval = conf.getInt("interval", 5);
		numVector = conf.getInt("vector", 0);
		dimension = conf.getInt("dimension", 0);
		logger = Logger.getLogger(NESpaceReducer.class);
		logger.setLevel(Level.WARN);
		minErrors = new double[numInterval * numVector];
		maxErrors = new double[numInterval * numVector]; 
		minFullError = new double[numVector];
		maxFullError = new double[numVector];
		
		fs = FileSystem.get(context.getConfiguration());
		
//		Path path = new Path(context.getConfiguration().get("keyPath"));
//		
//		os = fs.create(path);
		
		minRubnerKeys = new double[dimension];
		maxRubnerKeys = new double[dimension];
		
		List<Double[]> dualSolutions = FileUtil.readMultiFromHDFS(conf, conf.get("dualPath"));
		numDuals = dualSolutions.size();
		
		queryType = conf.get("query");
	}	
	
	@Override
	protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (int i = 0; i < numVector; i++) {
			minFullError[i] = Double.MAX_VALUE;
			maxFullError[i] = -Double.MAX_VALUE;
			
			for (int j = 0; j < numInterval; j++) {
				minErrors[i * numInterval + j] = Double.MAX_VALUE;
				maxErrors[i * numInterval + j] = -Double.MAX_VALUE;
			}
		}
		int eachErrorLength = 2 * numInterval + 1;
		
		for (int i = 0; i < dimension; i++) {
			minRubnerKeys[i] = Double.MAX_VALUE;
			maxRubnerKeys[i] = -Double.MAX_VALUE;
		}

		/*
		 * get min and max error for each interval and each vector
		 */
		int counter = 0;
		for (Text val : values) {
			logger.debug(key.toString() + " - " + val.toString());
			String first = "";
			String second = "";
			
			if (queryType.equals("rank") || queryType.equals("knn")) {
				first = val.toString().split(";")[0].trim();
				second = val.toString().split(";")[1].trim();
				
				double[] referenceFlows = FormatUtil.getDoubleArray(second, 0, second.split("\\s").length - 1);
				if(maxFlows == null) {
					maxFlows = new double[referenceFlows.length];
					for (int i = 0; i < maxFlows.length; i++) {
						maxFlows[i] = -Double.MAX_VALUE;
					}
				}
				for(int i = 0; i < referenceFlows.length; i++) {
					maxFlows[i] = maxFlows[i] > referenceFlows[i] ? maxFlows[i] : referenceFlows[i];
				}
			}
			else {
				first = val.toString();
			}
			//double[] allErrors = FormatUtil.getDoubleArray(val, 0, numVector * eachErrorLength - 1);
			double[] all = FormatUtil.getDoubleArray(first, 0, first.split("\\s").length - 1);
			double[] allErrors = FormatUtil.getSubArray(all, 0, numVector * eachErrorLength - 1);
			//double[] keys = FormatUtil.getSubArray(all, numVector * eachErrorLength, all.length - 1);
			//int numDuals = keys.length;
			double[] keys = FormatUtil.getSubArray(all, numVector * eachErrorLength, numVector * eachErrorLength + numDuals - 1); 
			double[] rubnerKeys = FormatUtil.getSubArray(all, numVector * eachErrorLength + numDuals, all.length - 1);
			
			/*
			 * initialize dual key array
			 */
			if (minKeys == null) {
				minKeys = new double[numDuals];
				maxKeys = new double[numDuals];
				
				for (int i = 0; i < numDuals; i++) {
					minKeys[i] = Double.MAX_VALUE;
					maxKeys[i] = -Double.MAX_VALUE;
				}
			}
			
			
			for (int i = 0; i < numVector; i++) {
				double[] errors = FormatUtil.getNthSubArray(allErrors, eachErrorLength, i);
				for (int j = 0; j < numInterval; j++) {
					int it = i * numInterval + j;
					minErrors[it] = errors[j * 2] < minErrors[it] ? errors[j * 2]
							: minErrors[it];
					maxErrors[it] = errors[j * 2 + 1] > maxErrors[it] ? errors[j * 2 + 1]
							: maxErrors[it];
				}
				minFullError[i] = errors[2 * numInterval] < minFullError[i] ? errors[2 * numInterval]
						: minFullError[i];
				maxFullError[i] = errors[2 * numInterval] > maxFullError[i] ? errors[2 * numInterval]
						: maxFullError[i];
			}
			counter++;	
			
			/*
			 * update min and max of dual keys
			 */
			for (int i = 0; i < numDuals; i++) {
				minKeys[i] = keys[i] > minKeys[i] ? minKeys[i] : keys[i];
				maxKeys[i] = keys[i] > maxKeys[i] ? keys[i] : maxKeys[i];
			}
			
			for (int i = 0; i < dimension; i++) {
				minRubnerKeys[i] = minRubnerKeys[i] < rubnerKeys[i] ? minRubnerKeys[i] : rubnerKeys[i];
				maxRubnerKeys[i] = maxRubnerKeys[i] > rubnerKeys[i] ? maxRubnerKeys[i] : rubnerKeys[i];
			}
			
		}
		/*
		 * write record (combination, count erros1 errors2 erros3 .. )
		 * each errors is of length 2 * interval + 2, min max min max ... min max minFull, maxFull
		 */
		String out = counter + " ";
		for (int i = 0; i < numVector; i++) {
			double[] eachMinErrors = FormatUtil.getNthSubArray(minErrors, numInterval, i);
			double[] eachMaxErrors = FormatUtil.getNthSubArray(maxErrors, numInterval, i);
			for (int j = 0; j < numInterval; j++) {
				out += eachMinErrors[j] + " ";
				out += eachMaxErrors[j] + " ";
			}
//			for (double err : eachMinErrors) {
//				out += err + " ";
//			}
			out += minFullError[i] + " ";
//			for (double err : eachMaxErrors) {
//				out += err + " ";
//			}
			out += maxFullError[i] + " ";
		}
		out = out.trim();
		
		if (queryType.equals("rank") || queryType.equals("knn")) {
			out += " " + FormatUtil.formatDoubleArray(maxFlows);
		}
		
		context.write(key, new Text(out));	
 		
		/*
		 * write out min and max of dual keys
		 */
		String randomName = Long.toHexString(Double.doubleToLongBits(Math.random()));
		Path path = new Path(context.getConfiguration().get("keyPath") + File.separator + randomName);
		os = fs.create(path);
		os.writeBytes(key + " " + FormatUtil.formatDoubleArray(minKeys) + " " + FormatUtil.formatDoubleArray(maxKeys) + "\n");
		os.flush();
		os.close();		
		
		/*
		 * write out min and max rubner keys
		 */
		randomName = Long.toHexString(Double.doubleToLongBits(Math.random()));
		Path rubnerPath = new Path(context.getConfiguration().get("rubnerPath") + File.separator + randomName);
		os = fs.create(rubnerPath);
		os.writeBytes(key + " " + FormatUtil.formatDoubleArray(minRubnerKeys) + " " + FormatUtil.formatDoubleArray(maxRubnerKeys) + "\n");
		os.flush();
		os.close();
	}
	
//	@Override
//	protected void cleanup(Context context) throws IOException {
//		os.flush();
//		os.close();
//	}
}
