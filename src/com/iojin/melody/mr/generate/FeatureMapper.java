package com.iojin.melody.mr.generate;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.iojin.melody.utils.ConfUtils;

public class FeatureMapper  extends Mapper<LongWritable, Text, Text, Text>{
	
	
	@Override 
	protected void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		String[] array = val.toString().split(ConfUtils.SEPARATOR);
		if (array.length < 2) {
			return;
		}
		Long id = Long.valueOf(StringUtils.trim(array[0]));
		for (int i = 1; i < array.length; i++) {
			StringBuilder builder = new StringBuilder();
			String[] feature = StringUtils.trim(array[i]).split("\\s");
			String featureName = StringUtils.trim(feature[0]);
			builder.append(id + " ");
			for (int j = 1; j < feature.length; j++) {
				builder.append(StringUtils.trim(feature[j]) + " ");
			}
			context.write(new Text(featureName), new Text(builder.toString()));
		}
	}
}
