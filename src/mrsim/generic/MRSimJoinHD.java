package mrsim.generic;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iojin.melody.mr.EmdJoin;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.TimerUtil;

public class MRSimJoinHD extends Configured implements Tool {

	private static final int pivotsIt = 0;
	private static final int reducerIt = 1;
	private static final int dimensionIt = 2;
	private static final int binsIt = 3;
	private static final int vectorIt = 4;
	private static final int epsIt = 5;
	private static final int inputPathIt = 6;
	private static final int binsPathIt = 7;
	private static final int vectorPathIt = 8;
	private static final int outputPathIt = 9;
	
	private static final int argsLength= outputPathIt - pivotsIt + 1;
	
	public static int getArgsLength() {
		return argsLength;
	}	
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		/**
		 * arguments:eps pivots reducer dimension binsLength inputPaths outputPaths binsPath 
		 * 
		 */
		TimerUtil.start();
		if(args.length != argsLength)
		{
			System.out.println("args length incorrect, length: " + args.length);
			return -1;
		}
		double eps;
		int numP, numReduces, dimension,binsLength, numVector;
		String binsPath, vectorPath,inputPaths,outputPaths;
		
		
		try
		{
			eps = new Double(args[epsIt]);
			System.out.println("eps set to: " + eps);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: eps not a type double");
			return -1;
		}
		try
		{
			numP = new Integer(args[pivotsIt]);
			System.out.println("number of pivots set to: " + numP);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: number of pivots not a type integer");
			return -1;
		}
		try
		{
			numReduces 	= new Integer(args[reducerIt]);
			System.out.println("number reducers set to: " + numReduces);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: number of reduces not a type integer");
			return -1;
		}
		
		try
		{
			dimension 	= new Integer(args[dimensionIt]);
			System.out.println("dimension set to: " + dimension);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: dimension not a type integer");
			return -1;
		}
		try
		{
			binsLength 	= new Integer(args[binsIt]) * dimension;
			System.out.println("binsLength set to: " + binsLength);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: binsLength not a type integer");
			return -1;
		}
		try
		{
			numVector 	= new Integer(args[vectorIt]);
			System.out.println("number of vector set to: " + numVector);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Error: binsLength not a type integer");
			return -1;
		}		
		inputPaths = args[inputPathIt];
		System.out.println("input path: "+inputPaths);
		
		outputPaths = args[outputPathIt];
		System.out.println("outputPaths: "+outputPaths);
		

		//FileUtil.deleteIfExistOnHDFS(getConf(), outputPaths);
		
		Configuration tmpConf = new Configuration();
		FileUtil.deleteIfExistOnHDFS(tmpConf, outputPaths);

		vectorPath = args[vectorPathIt];
		System.out.println("vectorPath: " + vectorPath);
		
		binsPath = args[binsPathIt];
		
		System.out.println("bins path: "+binsPath);

		
		String 		inputDir 		= inputPaths; 
		String 		outputDir		= outputPaths;
		//long 		memory 			= 131072; //128KB
		//long 		memory 			= 163840; //160KB
		//long 		memory 			= 196608; //192KB
		//long 		memory 			= 229376; //224KB
		//long 		memory 			= 262144; //256KB
		//long 		memory 			= 112 * 2730;
		long		memory			= 33554432; //32MB
		//long		memory			= 16777216; //16MB
		//long		memory			= 8388608; //8MB
		long 		itr				= 0;
		
		String 		intermediate	= outputDir + File.separator + "INTERMEDIATE/";
		String 		processed		= outputDir + File.separator  + "PROCCESSED/";
		String 		pivDir			= outputDir + File.separator + "PIVOTS/";
		
		while(true)
		{
			Configuration conf = new Configuration();
			conf.setBoolean("mapred.output.compress", false);
			conf.set("mapred.child.java.opts", "-Xmx2048M");
			conf.set("mapred.task.timeout", "8000000");
			conf.set("itr", "" + itr);
			conf.set("binsPath",binsPath);
			conf.set("vectorPath", vectorPath);
			String binsName = binsPath.substring(binsPath.lastIndexOf("/")+1,binsPath.length());
			binsPath = binsPath+"#"+binsName;
			
			conf.set("binsLength",String.valueOf(binsLength));
			conf.set("dimension",String.valueOf(dimension));
			conf.set("numVector", String.valueOf(numVector));
			
			FileSystem fs = FileSystem.get(conf);
			Path inputPath = new Path(inputDir);
			Path pivPath = new Path(pivDir + itr + "/pivots.txt");
			boolean isBaseRound = true;   ///need to determine if it is a base round or not
			
			if(itr == 0)
			{//first iteration, set input to primary input
				
				//make intermediate directory inside outputDir
				fs.mkdirs(new Path(intermediate));
				//make processed directory inside outputDir
				fs.mkdirs(new Path(processed));
				//set input path
				inputPath = new Path(inputDir);
			}
			else
			{
				//get first input directory in INTERMEDIATE
					// use listStatus(INTERMEDIATE)
				FileStatus[] fileStat = fs.listStatus(new Path(intermediate));
				System.out.println("fileStat length: " + fileStat.length);
				if(fileStat.length >= 1)
				{
					//set jobInputDir to first directory returned
					inputPath = fileStat[0].getPath();
					System.out.println("Input Path: " + inputPath.toString());
										
				}
				else
				{
					System.out.println("No input path");
					break;
				}
				
				
				int lastIndex = inputPath.toString().lastIndexOf("intermediate_output_");
		        String subBase = inputPath.toString().substring(lastIndex + 20);
		        StringTokenizer st = new StringTokenizer(subBase, "_");
		        int tokenCount = st.countTokens();
		        if(tokenCount == 2)//if tokenCount = 2, then base partition
		        {
		        	System.out.println("CheckRound: Base Round");
		            //it's a base round, set to true and continue
		        	isBaseRound = true;
		        }
		        else if(tokenCount == 3 || tokenCount ==4)//else if tokenCount = 3 window partion
		        {
		        	System.out.println("CheckRound: Window Round");
		        	isBaseRound = false;
		            //it's a window round, set to false and continue
		            st.nextToken();
		            String v = st.nextToken();
		            String w = st.nextToken();
		            conf.set("V", v);
		            conf.set("W", w);
		            
		        }
		        else
		        {
		            System.out.println("Directory Input Error");
		            break;
		        }
				
			}
			
			// Math3 dependency
			FileUtil.addDependency(conf);			
			DistributedCache.addCacheFile(new URI(binsPath),conf);
			DistributedCache.addCacheFile(new URI(vectorPath), conf);
			
			conf.set("eps", Double.toString(eps)); 				//assign string for eps to conf
			conf.set("memory", Long.toString(memory));			//assigns the variable mem to conf
			
			Object[] pivots = getPivots(inputPath, numP);		//generate pivot points from inputPath directory
			
			FSDataOutputStream out = fs.create(pivPath);		//write pivots to file
			
			for(int i = 0; i < pivots.length; i++)
			{
				out.writeBytes(((Text)pivots[i]).toString() + '\n');
			}
			//http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html#DistributedCache
			//http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html#Example%3A+WordCount+v2.0
			out.close();
			System.out.println("pivPath: " + pivPath.toString());
			
			
			Job job;
			if (isBaseRound)	//if it is a Base round
			{
				//execute base round MR on jobInputDir
				System.out.println("Base Round");
				System.out.println("InputDir:"+inputDir);
				//creates job
				job = new Job(conf, "MRSimJoinHD Base Round: Itr " + itr);
				System.out.println("---- Starting: "+job.getJobName());
				
				job.setNumReduceTasks(numReduces);
				
				
				//add file to cache
				DistributedCache.addCacheFile(pivPath.toUri(), job.getConfiguration());
				
				job.getConfiguration().set("outDir", intermediate);
				
				
				job.setJarByClass(EmdJoin.class);
				
				//sets mapper class
				job.setMapperClass(CloudJoinBaseMapper.class);
				//sets map output key/value types
			    job.setMapOutputKeyClass(CloudJoinKey.class);
			    job.setMapOutputValueClass(VectorElemHD.class);	
			    //sets Partitioner Class
			    job.setPartitionerClass(CloudJoinBasePartitioner.class);
			    //sets Sorting Comparitor class for job
			    job.setSortComparatorClass(CloudJoinBaseGrouper.class);
			    //Sets grouper class
			    job.setGroupingComparatorClass(CloudJoinBaseGrouper.class);
				//Set Reducer class
			    job.setReducerClass(CloudJoinBaseReducer.class);
			    // specify output types
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);	
				//sets output format
			    job.setInputFormatClass(TextInputFormat.class);
			    // specify input and output DIRECTORIES (not files)
				FileInputFormat.addInputPath(job, inputPath);
				FileOutputFormat.setOutputPath(job, new Path(outputDir + "/" + itr + "/"));
				
			}
			else	//else it is a window
			{
				//execute window round MR on jobInputDir
				System.out.println("Window Round");
				//set V
				//set M
				
				//creates job
				job = new Job(conf, "MRSimJoinHD Window Round: Itr " + itr);
				job.setNumReduceTasks(numReduces);
				//add file to cache
				DistributedCache.addCacheFile(pivPath.toUri(), job.getConfiguration());
				
				job.getConfiguration().set("outDir", intermediate);

				job.setJarByClass(EmdJoin.class);

				//sets mapper class
				job.setMapperClass(CloudJoinWindowMapper.class);
				//sets map output key/value types
			    job.setMapOutputKeyClass(CloudJoinKey.class);
			    job.setMapOutputValueClass(VectorElemHD.class);
			    //sets Partitioner Class
			    job.setPartitionerClass(CloudJoinWindowPartitioner.class);
			    //sets Sorting Comparitor class for job
			    job.setSortComparatorClass(CloudJoinWindowGrouper.class);
			    //Sets grouper class
			    job.setGroupingComparatorClass(CloudJoinWindowGrouper.class);
				//Set Reducer class
			    job.setReducerClass(CloudJoinWindowReducer.class);
			    // specify output types
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);	
				//sets output format
			    job.setInputFormatClass(TextInputFormat.class);
			    // specify input and output DIRECTORIES (not files)
				FileInputFormat.addInputPath(job, inputPath);
				FileOutputFormat.setOutputPath(job, new Path(outputDir + "/" + itr + "/"));
				
			}
			
			//start and wait till it finishes
			job.waitForCompletion(true);
			
			
			if (itr > 0)
			{
				// change jobInputDir from intermediate to processed
				//	fs.rename(Path src, Path dst);
				
				String dir = inputPath.toString();
				dir = dir.replace(intermediate, processed);
				Path newPath = new Path(dir);
				fs.rename(inputPath, newPath);
			}
			
			//increment itr for next round
			itr++;
			
			
		}
		
		TimerUtil.end();
		TimerUtil.print();
		return 0;
		
		
	}
	
	Object[] getPivots(Path input, int numPivs) throws IOException
	{
		
		JobConf job = new JobConf();
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, input);
		final KeyValueTextInputFormat inf = (KeyValueTextInputFormat) job.getInputFormat();
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(1.0, numPivs, 100);
		Object[] samples = sampler.getSample(inf, job);
		return samples;
	}
	

}
