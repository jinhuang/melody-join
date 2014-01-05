package com.iojin.melody.mr;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.FileUtil;
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
	private static String hibPath;
	private static String tempPath;
	private static String hashPath;
	private static String imagePath;
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		imagePath = conf.get(ConfUtils.GENERATEINPUT);
		hibPath = imagePath + "/" + HIB;
		tempPath = conf.get(ConfUtils.GENERATEOUTPUT) + "/" + HIST + "/" + TEMP;
		hashPath = conf.get(ConfUtils.GENERATEOUTPUT) + "/" + HASH; 
		
		generateHIB(conf);
		
		Job hashJob = new Job(conf);
		hashJob.setJarByClass(EmdGenerate.class);
		hashJob.setMapperClass(HashMapper.class);
		hashJob.setReducerClass(HashReducer.class);
		hashJob.setInputFormatClass(ImageBundleInputFormat.class);
		hashJob.setOutputKeyClass(Text.class);
		hashJob.setOutputValueClass(Text.class);
		hashJob.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(hashJob, hibPath);
		FileOutputFormat.setOutputPath(hashJob, new Path(hashPath));
		hashJob.waitForCompletion(true);
		FileUtil.addOutputToDistributedCache(conf, hashPath, HASH);
		
		Job job = new Job(conf);
		job.setJarByClass(EmdGenerate.class);
		job.setMapperClass(GenerateMapper.class);
		job.setReducerClass(GenerateReducer.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPaths(job, hibPath);
		FileOutputFormat.setOutputPath(job, new Path(tempPath));
		job.waitForCompletion(true);
		
		// cleanup
		FileUtil.deleteIfExistOnHDFS(conf, tempPath);
		FileUtil.deleteIfExistOnHDFS(conf, hibPath);
		
		return 0;
	}
	
	private void generateHIB(Configuration conf) throws IOException{
		FileUtil.deleteIfExistOnHDFS(conf, hibPath);
		FileUtil.deleteIfExistOnHDFS(conf, hibPath + HIB_DAT);
		HipiImageBundle hib = new HipiImageBundle(new Path(hibPath), conf);
		hib.open(AbstractImageBundle.FILE_MODE_WRITE, true);
		List<String> images = new ArrayList<String>();
		FileSystem fs = FileSystem.get(conf);
		FileUtil.getAllImagesOnHDFS(new Path(imagePath), fs, images);
		Long id = 0L;
		for (String each : images) {
			hib.addImage(fs.open(new Path(each)), ImageType.JPEG_IMAGE);
			id++;
		}
		hib.close();
	}

}
