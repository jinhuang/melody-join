package com.iojin.melody.utils;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.imageio.ImageIO;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.DocumentBuilderFactory;
import net.semanticmetadata.lire.imageanalysis.AutoColorCorrelogram;
import net.semanticmetadata.lire.imageanalysis.CEDD;
import net.semanticmetadata.lire.imageanalysis.ColorLayout;
import net.semanticmetadata.lire.imageanalysis.EdgeHistogram;
import net.semanticmetadata.lire.imageanalysis.FCTH;
import net.semanticmetadata.lire.imageanalysis.Gabor;
import net.semanticmetadata.lire.imageanalysis.JCD;
import net.semanticmetadata.lire.imageanalysis.JpegCoefficientHistogram;
import net.semanticmetadata.lire.imageanalysis.LireFeature;
import net.semanticmetadata.lire.imageanalysis.LuminanceLayout;
import net.semanticmetadata.lire.imageanalysis.OpponentHistogram;
import net.semanticmetadata.lire.imageanalysis.PHOG;
import net.semanticmetadata.lire.imageanalysis.ScalableColor;
import net.semanticmetadata.lire.imageanalysis.SimpleColorHistogram;
import net.semanticmetadata.lire.imageanalysis.Tamura;
import net.semanticmetadata.lire.imageanalysis.joint.JointHistogram;

public class GenerateUtil {
	
	private static final String ACC = "acc";
	private static final String CEDD = "cedd";
	private static final String CH = "ch";
	private static final String CL = "cl";
	private static final String EH = "eh";
	private static final String FCTH = "fcth";
	private static final String GABOR = "gabor";
	private static final String HCEDD = "hcedd";
	private static final String JCD = "jcd";
	private static final String JH = "jh";
	private static final String JCH = "jch";
	private static final String LL = "ll";
	private static final String OH = "oh";
	private static final String PHOG = "phog";
	private static final String SC = "sc";
	private static final String TAMURA = "tamura";
	
	private static final int FEATURE_USED = 0;
	
	public static double[] processImage(String imagePath, boolean local, int imageGrid, DocumentBuilder builder, 
			String featureName, FileSystem fs, double[] featureOutput, Map<String, LireFeature> lireFeatures) throws IOException, DecoderException {
		String imgName = getImageName(imagePath, local);
		BufferedImage eachImage = local ? ImageIO.read(new File(imagePath)) : ImageIO.read(fs.open(new Path(imagePath)));
		
		for (int i = 0; i < imageGrid; i++) {
			for (int j = 0; j < imageGrid; j++) {
				String subImgName = imgName + i + "," + j;
				Document document = builder.createDocument(getSubImageByRowAndColumn(eachImage, i, j, imageGrid), subImgName);
				featureOutput[i * imageGrid + j] = getFeatures(document, featureName, lireFeatures)[FEATURE_USED];
			}
		}
		
		// normalize
		featureOutput = HistUtil.normalizeArray(featureOutput);
		
		return featureOutput;
	}
	
	private static String getImageName(String imagePath, boolean local) {
		String separator = local ? File.separator : "/";
		String[] array = imagePath.split(separator);
		return array[array.length - 1].split("\\.")[0];
	}
	
	private static BufferedImage getSubImageByRowAndColumn(BufferedImage image, int row, int column, int imageGrid) {
		int height = image.getHeight() / imageGrid;
		int width = image.getWidth() / imageGrid;
		int upperLeftX = column * width;
		int upperLeftY = row * height;
		return image.getSubimage(upperLeftX, upperLeftY, width, height);
	}
	
	private static double[] getFeatures(Document document, String featureName, Map<String, LireFeature> lireFeatures) throws DecoderException {
		LireFeature feature = lireFeatures.get(featureName);
		feature.setByteArrayRepresentation(document.getFields().get(1).binaryValue().bytes);
		return feature.getDoubleHistogram();
	}
	
	public static void prepareFeaturesAndBuilders(Set<String> features, Map<String, DocumentBuilder> builders, Map<String, LireFeature> lireFeatures) {
		builders = new HashMap<String, DocumentBuilder>();
		lireFeatures = new HashMap<String, LireFeature>();
		if (features.contains(ACC)) {
			builders.put(ACC, DocumentBuilderFactory.getAutoColorCorrelogramDocumentBuilder());
			lireFeatures.put(ACC, new AutoColorCorrelogram());
		}
		if (features.contains(CEDD)) {
			builders.put(CEDD, DocumentBuilderFactory.getCEDDDocumentBuilder());
			lireFeatures.put(CEDD, new CEDD());
		}
		if (features.contains(CH)) {
			builders.put(CH, DocumentBuilderFactory.getColorHistogramDocumentBuilder());
			lireFeatures.put(CH, new SimpleColorHistogram());
		}
		if (features.contains(CL)) {
			builders.put(CL, DocumentBuilderFactory.getColorLayoutBuilder());
			lireFeatures.put(CL, new ColorLayout());
		}
		if (features.contains(EH)) {
			builders.put(EH, DocumentBuilderFactory.getEdgeHistogramBuilder());
			lireFeatures.put(EH, new EdgeHistogram());
		}
		if (features.contains(FCTH)) {
			builders.put(FCTH, DocumentBuilderFactory.getFCTHDocumentBuilder());
			lireFeatures.put(FCTH, new FCTH());
		}
		if (features.contains(GABOR)) {
			builders.put(GABOR, DocumentBuilderFactory.getGaborDocumentBuilder());
			lireFeatures.put(GABOR, new Gabor());
		}
		if (features.contains(HCEDD)) {
			builders.put(HCEDD, DocumentBuilderFactory.getHashingCEDDDocumentBuilder());
			lireFeatures.put(HCEDD, new CEDD());
		}
		if (features.contains(JCD)) {
			builders.put(JCD, DocumentBuilderFactory.getJCDDocumentBuilder());
			lireFeatures.put(JCD, new JCD());
		}
		if (features.contains(JH)) {
			builders.put(JH,DocumentBuilderFactory.getJointHistogramDocumentBuilder());
			lireFeatures.put(JH, new JointHistogram());
		}
		if (features.contains(JCH)) {
			builders.put(JCH, DocumentBuilderFactory.getJpegCoefficientHistogramDocumentBuilder());
			lireFeatures.put(JCH, new JpegCoefficientHistogram());
		}
		if (features.contains(LL)) {
			builders.put(LL, DocumentBuilderFactory.getLuminanceLayoutDocumentBuilder());
			lireFeatures.put(LL, new LuminanceLayout());
		}
		if (features.contains(OH)) {
			builders.put(OH, DocumentBuilderFactory.getOpponentHistogramDocumentBuilder());
			lireFeatures.put(OH, new OpponentHistogram());
		}
		if (features.contains(PHOG)) {
			builders.put(PHOG, DocumentBuilderFactory.getPHOGDocumentBuilder());
			lireFeatures.put(PHOG, new PHOG());
		}
		if (features.contains(SC)) {
			builders.put(SC, DocumentBuilderFactory.getScalableColorBuilder());
			lireFeatures.put(SC, new ScalableColor());
		}
		if (features.contains(TAMURA)) {
			builders.put(TAMURA, DocumentBuilderFactory.getTamuraDocumentBuilder());
			lireFeatures.put(TAMURA, new Tamura());
		}
	}
}
