package com.iojin.melody.mr.generate;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.util.ByteUtils;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.imageanalysis.LireFeature;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.iojin.melody.mr.EmdGenerate;
import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.FileUtil;
//import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.GenerateUtil;

public class GenerateMapper extends Mapper<ImageHeader, FloatImage, Text, Text> {
	private Set<String> featureNames;
	private Map<String, DocumentBuilder> builders;
	private Map<String, LireFeature> lireFeatures;
	private static int imageGrid;
	private static double[] featureOutput;
	private Map<String, Long> hashIds;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		imageGrid = conf.getInt(ConfUtils.GENERATEGRID, 4);
		featureOutput = new double[imageGrid * imageGrid];
		featureNames = new HashSet<String>();
		String[] array = conf.get(ConfUtils.GENERATEFEATURE).split("/");
		for (String each : array) {
			featureNames.add(StringUtils.trim(each.toLowerCase()));
		}
		System.out.println("selected " + featureNames.size() + " features");
		builders = new HashMap<String, DocumentBuilder>();
		lireFeatures = new HashMap<String, LireFeature>();
		GenerateUtil.prepareFeaturesAndBuilders(featureNames, builders, lireFeatures);
		hashIds = FileUtil.getHashFromDistributedCache(conf, EmdGenerate.HASH);
	}
	
	@Override
	protected void map(ImageHeader header, FloatImage image, Context context) throws IOException, InterruptedException {
		
		String hexHash = ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(image.getData()));
		Long id = hashIds.get(hexHash);
		BufferedImage img = GenerateUtil.JPEGFloatImageToBufferedImage(image, header);
		
		for (String featureName : builders.keySet()) {
			DocumentBuilder builder = builders.get(featureName);
			try {
				featureOutput = GenerateUtil.processImage(img, "",imageGrid, builder, featureName, featureOutput, lireFeatures);
				context.write(new Text(featureName), new Text(id + " " + FormatUtil.toTextString(featureOutput)));
			} catch (DecoderException e) {
				e.printStackTrace();
			}
		}			
	}
}