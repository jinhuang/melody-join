package com.iojin.melody.mr;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import net.semanticmetadata.lire.utils.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iojin.melody.Generate;
import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.mr.generate.CrawlMapper;
import com.iojin.melody.mr.generate.CrawlReducer;
import com.iojin.melody.mr.generate.FeatureMapper;
import com.iojin.melody.mr.generate.FeatureReducer;
import com.iojin.melody.mr.generate.GenerateMapper;
import com.iojin.melody.mr.generate.GenerateReducer;
import com.iojin.melody.mr.generate.HashMapper;
import com.iojin.melody.mr.generate.HashReducer;

public class EmdGenerate extends Configured implements Tool{

	private static final String HIB = "image.hib";
	private static final String HIB_DAT = ".dat";
	public static final String HIST = "hist";
	private static final String TEMP = "temp";
	public static final String HASH = "hash";
	private static String idMapPath;
	private static String hibPath;
	private static String tempPath;
	private static String hashPath;
	private static String imagePath;
	private static boolean crawlmr;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		imagePath = conf.get(ConfUtils.GENERATEINPUT);
		hibPath = conf.get(ConfUtils.GENERATEOUTPUT) + "/" + HIB;
		tempPath = conf.get(ConfUtils.GENERATEOUTPUT) + "/" + HIST + "/" + TEMP;
		hashPath = conf.get(ConfUtils.GENERATEOUTPUT) + "/" + HASH;
		idMapPath = conf.get(ConfUtils.GENERATEOUTPUT) + "/" + Generate.META_ID;
		crawlmr = conf.get(ConfUtils.GENERATEMRINPUT).equalsIgnoreCase(ConfUtils.GENERATEMRHTTP);
		
		if (crawlmr) { // the input is a url list on HDFS
			
			Job crawlJob = new Job(conf);
			crawlJob.setJobName("Crawling and Processing Images From URL List");
			crawlJob.setJarByClass(EmdGenerate.class);
			crawlJob.setMapperClass(CrawlMapper.class);
			crawlJob.setReducerClass(CrawlReducer.class);
			crawlJob.setMapOutputKeyClass(LongWritable.class);
			crawlJob.setMapOutputValueClass(Text.class);
			crawlJob.setOutputKeyClass(LongWritable.class);
			crawlJob.setOutputValueClass(Text.class);
			
			// enforce the number of reducer to be the parallel degree specified
			crawlJob.setNumReduceTasks(conf.getInt(ConfUtils.TASK, 48));
			
			// use the id - url hash file instead of the original file 
			FileInputFormat.addInputPaths(crawlJob, idMapPath);
			
			FileUtil.deleteIfExistOnHDFS(conf, tempPath);
			FileOutputFormat.setOutputPath(crawlJob, new Path(tempPath));
			crawlJob.waitForCompletion(true);
			
			
			Job featureJob = new Job(conf);
			featureJob.setJobName("Gathering Features Into Files");
			featureJob.setJarByClass(EmdGenerate.class);
			featureJob.setMapperClass(FeatureMapper.class);
			featureJob.setReducerClass(FeatureReducer.class);
			featureJob.setMapOutputKeyClass(Text.class);
			featureJob.setMapOutputValueClass(Text.class);
			featureJob.setOutputKeyClass(LongWritable.class);
			featureJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(featureJob, new Path(tempPath));
			FileUtil.deleteIfExistOnHDFS(conf, tempPath + "/feature");
			FileOutputFormat.setOutputPath(featureJob, new Path(tempPath + "/feature"));
			featureJob.waitForCompletion(true);
			
			// delete the directory as the job use bufferwriter instead of context.write
			// to output the result
			FileUtil.deleteIfExistOnHDFS(conf, tempPath);
			
			
			return 0;
		}
		else { // the input is a directory of image files on HDFS
			generateHIB(conf);
			
			Job hashJob = new Job(conf);
			hashJob.setJobName("Generating Hashing Id Map");
			hashJob.setJarByClass(EmdGenerate.class);
			hashJob.setMapperClass(HashMapper.class);
			hashJob.setReducerClass(HashReducer.class);
			hashJob.setInputFormatClass(ImageBundleInputFormat.class);
			hashJob.setOutputKeyClass(Text.class);
			hashJob.setOutputValueClass(Text.class);
			hashJob.setNumReduceTasks(1);
			FileInputFormat.addInputPaths(hashJob, hibPath);
			FileUtil.deleteIfExistOnHDFS(conf, hashPath);
			FileOutputFormat.setOutputPath(hashJob, new Path(hashPath));
			hashJob.waitForCompletion(true);
			FileUtil.addOutputToDistributedCache(conf, hashPath, HASH);
			
			Job job = new Job(conf);
			job.setJobName("Processing Images");
			job.setJarByClass(EmdGenerate.class);
			job.setMapperClass(GenerateMapper.class);
			job.setReducerClass(GenerateReducer.class);
			job.setInputFormatClass(ImageBundleInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPaths(job, hibPath);
			FileUtil.deleteIfExistOnHDFS(conf, tempPath);
			FileOutputFormat.setOutputPath(job, new Path(tempPath));
			job.waitForCompletion(true);
			
			// cleanup
			FileUtil.deleteIfExistOnHDFS(conf, tempPath);
			
			return 0;
		}
	}
	
	private void generateHIB(Configuration conf) throws IOException{
		if (FileUtil.existOnHDFS(conf, hibPath)) {
			return;
		}
		FileUtil.deleteIfExistOnHDFS(conf, hibPath);
		FileUtil.deleteIfExistOnHDFS(conf, hibPath + HIB_DAT);
		HipiImageBundle hib = new HipiImageBundle(new Path(hibPath), conf);
		hib.open(AbstractImageBundle.FILE_MODE_WRITE, true);
		List<String> images = new ArrayList<String>();
		if (conf.get(ConfUtils.GENERATEMRINPUT).equalsIgnoreCase(ConfUtils.GENERATEMRHDFS)) {
			FileSystem fs = FileSystem.get(conf);
			FileUtil.getAllImagesOnHDFS(new Path(imagePath), fs, images);
			for (String each : images) {
				hib.addImage(fs.open(new Path(each)), ImageType.JPEG_IMAGE);
			}
		}
		else if (conf.get(ConfUtils.GENERATEMRINPUT).equalsIgnoreCase(ConfUtils.GENERATEMRLOCAL)) {
			File file = new File(imagePath);
			images = FileUtils.getAllImages(file, true);
			for (String each : images) {
				hib.addImage(new FileInputStream(each), ImageType.JPEG_IMAGE);
			}			
		}
		hib.close();
	}

}
