package com.iojin.melody.utils;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ConfUtils {
	
	public static final String CACHED = "bsp.cached";
	public static final String BATCH = "bsp.batch.message";
	public static final String DEPENDENCY = "dependency.math.hdfs.path";
	public static final String DIMENSION = "data.dimension";
	public static final String NUMBIN = "data.bin.number";
	public static final String INPUT = "data.input.hdfs.path";
	public static final String OUTPUT = "data.output.hdfs.path";
	public static final String BIN = "data.bin.hdfs.path";
	public static final String VECTOR = "melody.project.vector.hdfs.path";
	public static final String NUMVECTOR = "melody.project.vector.number";
	public static final String GRID = "melody.grid.cell.granularity";
	public static final String QUERY = "melody.join.type";
	public static final String THRESHOLD = "melody.join.distance.threshold";
	public static final String PARAK = "melody.join.k";
	public static final String METHOD = "mr.method.name";
	public static final String INTERVAL = "melody.normal.error.interval";
	public static final String TASK = "parallel.task.number";
	public static final String PIVOT = "mrsim.pivot.number";
	public static final String RATIO = "melody.sample.ratio";
	
	private static Configuration instance;
	
	public static Configuration getInstance() throws ConfigurationException {
		if (instance == null) {
			instance = readConf();
		}
		return instance;
	}
	
	public static Configuration getInstance(String path) throws ConfigurationException {
		if (instance == null) {
			instance = readConf(path);
		}
		return instance;
	}
	
	public static void loadConf(String path) throws ConfigurationException {
		instance = readConf(path);
	}
	
	public static String getString(String key) throws ConfigurationException {
		if (instance == null) readConf();
		return instance.getString(key);
	}
	
	public static boolean getBoolean(String key) throws ConfigurationException {
		if (instance == null) readConf();
		return instance.getBoolean(key);
	}
	
	public static double getDouble(String key) throws ConfigurationException {
		if (instance == null) readConf();
		return instance.getDouble(key);
	}
	
	public static int getInteger(String key) throws ConfigurationException {
		if (instance == null) readConf();
		return instance.getInt(key);
	}
	
	protected ConfUtils (){
		
	}
	
	private static Configuration readConf() throws ConfigurationException {
		return new PropertiesConfiguration("melody-conf.properties");
	}
	
	private static Configuration readConf(String path) throws ConfigurationException {
		return new PropertiesConfiguration(new File(path));
	}	
	
}
