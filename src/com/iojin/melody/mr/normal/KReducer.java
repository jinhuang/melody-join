package com.iojin.melody.mr.normal;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iojin.melody.utils.JoinedPair;

public class KReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	private static int paraK;
	private static TreeSet<JoinedPair> joinedPairs = new TreeSet<JoinedPair>();
	
	@Override
	protected void setup(Context context) {
		paraK = context.getConfiguration().getInt("paraK", 0);
	}
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
			System.out.println(key + " : " + val);
			joinedPairs.add(new JoinedPair(val.toString()));
		}
		
		System.out.println("Key " + key.toString() + " : " + joinedPairs.size());
		Iterator<JoinedPair> it = joinedPairs.iterator();
		for (int i = 0; i < paraK; i++) {
			JoinedPair pair = it.next();
			context.write(new LongWritable(i), new Text(pair.toString()));
		}
		joinedPairs.clear();
	}
}
