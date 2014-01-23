package com.iojin.melody.utils;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

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
	
	public static final String GENERATEMODE = "generate.mode";
	public static final String GENERATEMRINPUT = "generate.mr.input";
	public static final String GENERATEHDFS = "generate.hdfs.name";
	public static final String GENERATEINPUT = "generate.input.image.dir";
	public static final String GENERATEOUTPUT = "generate.output.hist.dir";
	public static final String GENERATEFEATURE = "generate.enabled.features";
	public static final String GENERATEGRID = "generate.grid.granularity";
	public static final String GENERATEFEATUREVALUE = "generate.feature.value";
	public static final String GENERATECRAWLFREQ = "generate.crawl.freq";
	
	public static final String GENERATELOCAL = "local";
	public static final String GENERATEMR = "mr";
	public static final String GENERATEMRLOCAL = "local";
	public static final String GENERATEMRHDFS = "hdfs";
	public static final String GENERATEMRHTTP = "http";
	
	public static final String GENERATEPERTASK = "generate.pertask";
	
	public static final String SEPARATOR = "/";
	
	public static final String ID_HASH = "=>";
	
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
	
	public static Set<String> getStringSet(String key) throws ConfigurationException {
		HashSet<String> set = new HashSet<String>();
		String[] array = ConfUtils.getString(key).split(SEPARATOR);
		for (String each : array) {
			set.add(StringUtils.trim(each.toLowerCase()));
		}
		return set;
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
