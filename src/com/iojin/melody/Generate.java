package com.iojin.melody;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.imageanalysis.LireFeature;
import net.semanticmetadata.lire.utils.FileUtils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.iojin.melody.mr.EmdGenerate;
import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.GenerateUtil;

public class Generate {
	private static boolean local;
	private static String inputDir;
	private static String outputDir;
	private static Set<String> featureNames;
	private static Map<String, DocumentBuilder> builders;
	private static Map<String, LireFeature> lireFeatures;
	private static Map<Long, String> idMap;
	private static List<String> images;
	private static Configuration conf;
	private static FileSystem fs;
	private static int imageGrid;
	private static double[] featureOutput;
	
	private static final int DIMENSION = 2;
	
	private static final String META_ID = "id";
	private static final String META_BIN = "bins";
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("USAGE: <conf.properties path>");
			return;
		}
		if (!new File(args[1]).exists()) {
			System.out.println("cannot find the specified .properties file");
			return;
		}
		
		ConfUtils.loadConf(args[1]);
		
		local = ConfUtils.getString(ConfUtils.GENERATEMODE).equalsIgnoreCase("local");
		inputDir = ConfUtils.getString(ConfUtils.GENERATEINPUT);
		outputDir = ConfUtils.getString(ConfUtils.GENERATEOUTPUT);
		featureNames = ConfUtils.getStringSet(ConfUtils.GENERATEFEATURE);
		imageGrid = ConfUtils.getInteger(ConfUtils.GENERATEGRID);
		featureOutput = new double[imageGrid * imageGrid];
		builders = new HashMap<String, DocumentBuilder>();
		lireFeatures = new HashMap<String, LireFeature>();
		GenerateUtil.prepareFeaturesAndBuilders(featureNames, builders, lireFeatures);
		
		prepareDirectory();
		
		// MapReduce
		if (ConfUtils.getString(ConfUtils.GENERATEMODE).equalsIgnoreCase("mr")) {
			setConf(ConfUtils.GENERATEFEATURE);
			setConf(ConfUtils.GENERATEGRID);
			setConf(ConfUtils.GENERATEINPUT);
			setConf(ConfUtils.GENERATEOUTPUT);
			EmdGenerate generator = new EmdGenerate();
			
			System.exit(ToolRunner.run(conf, generator, null));
		}
		// Local Processing
		else {
			for (String featureName : builders.keySet()) {
				DocumentBuilder builder = builders.get(featureName);
				String dir = local ? outputDir + File.separator + featureName : outputDir + "/" + featureName;
				Long id = 0L;
				for (String each : images) {
					featureOutput = GenerateUtil.processImage(each, local, imageGrid, builder, featureName, fs, featureOutput, lireFeatures);
					write(dir, id + " " + FormatUtil.toTextString(featureOutput));
					id++;	
				}
			}
		}
	}
	
	private static void prepareDirectory() throws IOException, ConfigurationException {
		if (local) {
			File input = new File(inputDir);
			if (!input.exists() || !input.isDirectory()) {
				System.out.println(inputDir + " is not a directory");
				return;
			}
			File output = new File(outputDir);
			if (output.exists()) {
				output.delete();
			}
			output.mkdirs();
			images = FileUtils.getAllImages(input, true);
		}
		else {
			conf = new Configuration();
			conf.set("fs.default.name", ConfUtils.getString(ConfUtils.GENERATEHDFS));
			fs = FileSystem.get(conf);
			Path inPath = new Path(inputDir);
			if (!fs.exists(inPath) || !fs.getFileStatus(inPath).isDir()) {
				System.out.println(inputDir + " on " + ConfUtils.getString(ConfUtils.GENERATEHDFS) + " is not a directory");
			}
			FileUtil.deleteIfExistOnHDFS(conf, outputDir);
			fs.mkdirs(new Path(outputDir));
			images = new ArrayList<String>();
			FileUtil.getAllImagesOnHDFS(inPath, fs, images);
		}
		
		// write out id - file mapping metadata
		idMap = new HashMap<Long, String>();
		Long id = 0L;
		for (String each : images) {
			idMap.put(id, each);
			id++;
		}
		String idOutput = local ? outputDir + File.separator + META_ID : outputDir + "/" + META_ID;
		write(idOutput, idMap);
		
		// write out bins metadata
		// e.g. 10 * 10 bins[0] -> bin0d0, bins[1] -> bin0d1, bins[2] => bin1d0
		int[] bins = new int[imageGrid * imageGrid * DIMENSION];
		for (int i = 0; i < imageGrid * imageGrid; i++) {
			bins[i * DIMENSION] = i / imageGrid;
			bins[i * DIMENSION + 1] = i % imageGrid;
		}
		String binOutput = local ? outputDir + File.separator + META_BIN : outputDir + "/" + META_BIN;
		write(binOutput, FormatUtil.toTextString(bins));
	}
	
	private static void write(String path, String content) throws IOException {
		if (local) {
			FileUtil.writeContent(path, content);
		}
		else {
			FileUtil.writeContentToHDFS(path, content, fs);
		}
	}
	
	private static void write(String path, Map<Long, String> map) throws IOException {
		if (local) {
			FileUtil.writeContent(path, map);
		}
		else {
			FileUtil.writeContentToHDFS(path, map, fs);
		}
	}
	
	private static void setConf(String key) throws ConfigurationException {
		conf.set(key, ConfUtils.getString(key));
	}
}
