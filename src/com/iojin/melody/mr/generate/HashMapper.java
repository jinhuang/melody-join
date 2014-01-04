package com.iojin.melody.mr.generate;

import java.io.IOException;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.util.ByteUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashMapper extends Mapper<ImageHeader, FloatImage, Text, Text> {
	
	private Long id = 0L;
	
	@Override
	protected void map(ImageHeader header, FloatImage image, Context context) throws IOException, InterruptedException {
		String hexHash = ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(image.getData()));
		context.write(new Text(hexHash), new Text(id.toString()));
		id++;
	}
}
