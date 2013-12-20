package com.iojin.melody.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtil {
	
//	private static final String mathDependency = "s3n://emdjoin/commons-math3-3.1.1.jar";
	
	private static String mathDependency = "/emdjoin/dependency/commons-math3-3.1.1.jar";
	
	public static void setMathDependency(String path) {
		mathDependency = path;
	}
	
	
	public static void writeContent(String path, String content) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
		writer.write(content + "\n");
//		System.out.println(file.getAbsolutePath());
		writer.close();
	}
	
	public static void writeContent(String path, Map<Long, String> map) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
		for (Long id : map.keySet()) {
			writer.write(id + " => " + map.get(id) + "\n");
		}
		writer.close();
	}
	
	public static void writeContentToHDFS(String out, String content, FileSystem fs) throws IOException {
		Path path = new Path(out);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
		writer.write(content + "\n");
		writer.close();
	}
	
	public static void writeContentToHDFS(String out, Map<Long, String> map, FileSystem fs) throws IOException {
		Path path = new Path(out);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
		for (Long id : map.keySet()) {
			writer.write(id + " => " + map.get(id) + "\n");
		}
		writer.close();
	}	
	
	public static double [] getData(String addr,int size){
		double [] datas = new double[size];
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(addr));
			String[] tokens = br.readLine().split("\\s");
			for (int i = 0; i < size; i++) {
				tokens[i] = tokens[i].trim();
				if (tokens[i].length() > 0) {
					datas[i] = Double.valueOf(tokens[i]);
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return datas;		
	}
	
	public static List<Double> getMultiData(String addr,int size){
		List<Double> data = new ArrayList<Double>();
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(addr));
			String line = br.readLine();
			while(null != line) {
				String[] eles = line.split("\\s");
				for (int j = 0; j < size; j++) {
					data.add(Double.valueOf(eles[j]));
				}
				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return data;		
	}	
	
	public static double[] getAdjustedBins(double[] bins){
		double sum = 0;
		for(double d:bins){
			sum += d;
		}
		double means = sum/bins.length;
		
		for(int i=bins.length-1;i>=0;i--)
			bins[i] -= means;
		
		return bins;
	}
	
	public static String getNameFromPath(String path) {
		return path.substring(path.lastIndexOf("/")+1, path.length());
	}
	
	public static double[] getFromDistributedCache(Configuration conf, String name, int length) throws IOException {
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		double[] vector = new double[length];
		for(Path path:localFiles){
			if(path.toString().indexOf(name) > -1){
				vector = getData(path.toString(), vector.length);	
			}
		}
		return vector;
	}
	
	public static double[] getMultiFromDistributedCache(Configuration conf, String name, int lineLength) throws IOException {
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		List<Double> list = new ArrayList<Double>();
		for(Path path:localFiles){
			if(path.toString().indexOf(name) > -1){
				File file = new File(path.toString());
				if (!file.isDirectory()) {
					list.addAll(getMultiData(path.toString(), lineLength));	
				}
			}
		}
		double[] array = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = list.get(i);
		}
		return array;
	}
	
	public static void addOutputToDistributedCache(Configuration conf, String outputPath, String name) throws IOException, URISyntaxException {
		Path path = new Path(outputPath);
		FileSystem fileSystem = FileSystem.get(conf);
		FileStatus status[] = fileSystem.listStatus(path);
		for (FileStatus file : status) {
			if (file.getPath().toString().indexOf("part-r") > -1) {
				String namePath = addName(file.getPath().toString(), name);
				fileSystem.rename(file.getPath(), new Path(namePath));
				DistributedCache.addCacheFile(new URI(namePath), conf);
			}
		}
	}	
	
	public static void deleteIfExistOnHDFS(Configuration conf, String path) {
		// commented out for s3 operation 0705
		Path pt = new Path(path);
		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(pt)) {
				fs.delete(pt, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public static void getAllImagesOnHDFS(Path path, FileSystem fs, List<String> list) throws IOException {
		for (FileStatus each : fs.listStatus(path)) {
			if (each.isDir()) {
				getAllImagesOnHDFS(each.getPath(), fs, list);
			}
			else {
				list.add(each.getPath().toString());
			}
		}
	}
	
	// for S3
//	public static double[] readFromHDFS(Configuration conf, String path) {
//		Path pt = new Path(path);
//		double[] result = null;
//		try {
//			FileSystem fs = FileSystem.get(conf);
//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
//			String line = br.readLine();
//			//System.out.println(line);
//			result = FormatUtil.toDoubleArray(line);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
	
	public static List<Double[]> readMultiFromHDFS(Configuration conf, String path) {
		Path pt = new Path(path);
		List<Double[]> arrayResult = new ArrayList<Double[]>(); 
		try {
			FileSystem fs = FileSystem.get(conf);
			FileStatus status[] = fs.listStatus(pt);
			for (FileStatus file : status) {
//				System.out.println("read from " + file.getPath());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
				String line = br.readLine();
				while(null != line) {
					arrayResult.add(FormatUtil.toObjectDoubleArray(line));
					line = br.readLine();
				}
				br.close();
			}			
			//System.out.println(line);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return arrayResult;
	}
	
	public static String readSingleFromHDFS(Configuration conf, String path) {
		Path pt = new Path(path);
		String line = "0.0";
		try {
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			line = br.readLine();
			br.close();
		} catch(IOException e) {
			e.printStackTrace();
		} 
		return line;
	}
	
	public static List<Double[]> readArraysLocalFile(String path) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
		List<Double[]> result = new ArrayList<Double[]>();
		String line = "";
		while((line = br.readLine()) != null) {
			result.add(FormatUtil.toObjectDoubleArray(FormatUtil.toDoubleArray(line)));
		}
		br.close();
		return result;
	}

	
	private static String addName(String path, String name) {
		return path.substring(0, path.lastIndexOf("/") + 1) + name + "-" + path.substring(path.lastIndexOf("/") + 1, path.length());
	}
	
	public static void addDependency(Configuration conf) throws IOException, URISyntaxException {
		FileSystem fileSystem = FileSystem.get(conf);
		DistributedCache.addFileToClassPath(new Path(mathDependency), conf, fileSystem);
	}
	
}
