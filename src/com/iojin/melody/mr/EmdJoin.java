package com.iojin.melody.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.iojin.melody.mr.normal.QuantileNormalEmd;

import mrsim.generic.*;

public class EmdJoin {
	
	private static final int COMMAND_ARGS = 0;
	
	private static String[] parseArgs(String[] args, MethodType type) {
		String[] methodArgs;
		switch(type) {
			case QuantileNormalEmd:
				methodArgs = new String[QuantileNormalEmd.getArgLength() + COMMAND_ARGS];
				break;
			case MRSimJoin:
				methodArgs = new String[MRSimJoinHD.getArgsLength() + COMMAND_ARGS];
				break;
			default:
				methodArgs = new String[0];
				break;
		}
		for (int i = 1; i < args.length; i++) {
			methodArgs[i-1] = args[i];
		}	
		return methodArgs;
	}
	
	public static void disableTimeout(Configuration conf) {
		conf.setLong("mapred.task.timeout", 0);
	}
	
	public static void main(String[] args) throws Exception{
		
		int resCode = 0;
		QuantileNormalEmd quantile = new QuantileNormalEmd();
		MRSimJoinHD mrSimJoin = new MRSimJoinHD();
		
		Configuration conf = new Configuration();

		if (args[0].equals("qne")) {
			String[] qneArgs = parseArgs(args, MethodType.QuantileNormalEmd);
			resCode = ToolRunner.run(conf, quantile, qneArgs);
		}
		else if (args[0].equals("mrs")) {
			String[] mrsArgs = parseArgs(args, MethodType.MRSimJoin);
			resCode = ToolRunner.run(conf, mrSimJoin, mrsArgs);
		}
		switch(resCode) {
			case -1:
				System.out.println("Invalid Arguments");
				break;
			default:
				break;
		}
		System.exit(resCode);
	}
}
