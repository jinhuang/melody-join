package com.iojin.melody;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;

import com.iojin.melody.bsp.Baseline;
import com.iojin.melody.bsp.Normal;
import com.iojin.melody.mr.EmdJoin;
import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.FileUtil;

public class Join {
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("USAGE: <conf.properties path>");
			return;
		}
		if (!new File(args[0]).exists()) {
			System.out.println("cannot find the specified .properties file");
			return;
		}
		
		try {
			ConfUtils.loadConf(args[0]);
			String method = ConfUtils.getString(ConfUtils.METHOD);
			int grid = ConfUtils.getInteger(ConfUtils.GRID);
			int interval = ConfUtils.getInteger(ConfUtils.INTERVAL);
			int worker = ConfUtils.getInteger(ConfUtils.TASK);
			int dimension = ConfUtils.getInteger(ConfUtils.DIMENSION);
			int numBin = ConfUtils.getInteger(ConfUtils.NUMBIN);
			int numVector = ConfUtils.getInteger(ConfUtils.NUMVECTOR);
			String input = ConfUtils.getString(ConfUtils.INPUT);
			String bin = ConfUtils.getString(ConfUtils.BIN);
			String vector = ConfUtils.getString(ConfUtils.VECTOR);
			String output = ConfUtils.getString(ConfUtils.OUTPUT);
			int ratio = (int) (1 / ConfUtils.getDouble(ConfUtils.RATIO));
			
			FileUtil.setMathDependency(ConfUtils.getString(ConfUtils.DEPENDENCY));
			
			String[] passArgs = null;
			if (method.equalsIgnoreCase("melody")) {
				method = "qne";
				String query = ConfUtils.getString(ConfUtils.QUERY);
				passArgs = new String[14];
				if (query.equalsIgnoreCase("distance")) {
					query = "range";
					double threshold = ConfUtils.getDouble(ConfUtils.THRESHOLD);
					passArgs[7] = String.valueOf(threshold);
				}
				else if (query.equalsIgnoreCase("topk")) {
					query = "rank";
					int paraK = ConfUtils.getInteger(ConfUtils.PARAK);
					passArgs[7] = String.valueOf(paraK);
				}
				passArgs[0] = method;
				passArgs[1] = String.valueOf(grid);
				passArgs[2] = String.valueOf(interval);
				passArgs[3] = String.valueOf(worker);
				passArgs[4] = String.valueOf(dimension);
				passArgs[5] = String.valueOf(numBin);
				passArgs[6] = String.valueOf(numVector);
				passArgs[8] = query;
				passArgs[9] = String.valueOf(ratio);
				passArgs[10] = input;
				passArgs[11] = bin;
				passArgs[12] = vector;
				passArgs[13] = output;
				EmdJoin.main(passArgs);
			}
			else if (method.equalsIgnoreCase("mrsim")) {
				method = "mrs";
				passArgs = new String[11];
				passArgs[0] = method;
				passArgs[1] = String.valueOf(ConfUtils.getInteger(ConfUtils.PIVOT));
				passArgs[2] = String.valueOf(worker);
				passArgs[3] = String.valueOf(dimension);
				passArgs[4] = String.valueOf(numBin);
				passArgs[5] = String.valueOf(numVector);
				passArgs[6] = String.valueOf(ConfUtils.getDouble(ConfUtils.THRESHOLD));
				passArgs[7] = input;
				passArgs[8] = bin;
				passArgs[9] = vector;
				passArgs[10] = output;
				EmdJoin.main(passArgs);
			}
			else if (method.equalsIgnoreCase("bspmelody")) {
				passArgs = new String[12];
				passArgs[0] = String.valueOf(worker);
				passArgs[1] = String.valueOf(ConfUtils.getInteger(ConfUtils.PARAK));
				passArgs[2] = String.valueOf(dimension);
				passArgs[3] = String.valueOf(numBin);
				passArgs[4] = String.valueOf(numVector);
				passArgs[5] = String.valueOf(grid);
				passArgs[6] = input;
				passArgs[7] = bin;
				passArgs[8] = vector;
				passArgs[9] = output;
				passArgs[10] = String.valueOf(ConfUtils.getBoolean(ConfUtils.CACHED));
				passArgs[11] = String.valueOf(ConfUtils.getInteger(ConfUtils.BATCH));
				Normal.main(passArgs);
			}
			else if (method.equalsIgnoreCase("bspb")) {
				passArgs = new String[11];
				passArgs[0] = String.valueOf(worker);
				passArgs[1] = String.valueOf(ConfUtils.getInteger(ConfUtils.PARAK));
				passArgs[2] = String.valueOf(dimension);
				passArgs[3] = String.valueOf(numBin);
				passArgs[4] = String.valueOf(numVector);
				passArgs[5] = input;
				passArgs[6] = bin;
				passArgs[7] = vector;
				passArgs[8] = output;
				passArgs[9] = String.valueOf(ConfUtils.getBoolean(ConfUtils.CACHED));
				passArgs[10] = String.valueOf(ConfUtils.getInteger(ConfUtils.BATCH));
				Baseline.main(passArgs);				
			}
			else {
				System.out.println("method " + method + " not suppoted");
				return;
			}
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
