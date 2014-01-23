package com.iojin.melody.mr.generate;

import java.awt.color.CMMException;
import java.awt.image.BufferedImage;
import java.awt.image.RasterFormatException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.lang.StringBuilder;

import javax.imageio.ImageIO;
import javax.net.ssl.SSLException;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.imageanalysis.LireFeature;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.HttpEntity;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.conn.ConnectTimeoutException;

import com.iojin.melody.utils.ConfUtils;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.GenerateUtil;

public class CrawlReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	
	private BufferedImage image;
	private int featureUsed;
	private int freq;
	
	private Set<String> featureNames;
	private Map<String, DocumentBuilder> builders;
	private Map<String, LireFeature> lireFeatures;
	private static int imageGrid;
	private static double[] featureOutput;
	
	private static RequestConfig reqConf;
	private static CloseableHttpResponse response;
	private static CloseableHttpClient httpClient = HttpClients.createDefault();
	private static ExecutorService executor;
	
	private static final String MIME_JPG = "image/jpeg";
	
	private static final int TIMEOUT_SOCKET_MILLI = 5000;
	private static final int TIMEOUT_CONNECT_MILLI = 5000;
	private static final int TIMEOUT_TASK_SEC = 10;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		imageGrid = conf.getInt(ConfUtils.GENERATEGRID, 4);
		featureOutput = new double[imageGrid * imageGrid];
		featureNames = new HashSet<String>();
		String[] array = conf.get(ConfUtils.GENERATEFEATURE).split(ConfUtils.SEPARATOR);
		for (String each : array) {
			featureNames.add(StringUtils.trim(each.toLowerCase()));
		}
		System.out.println("selected " + featureNames.size() + " features");
		builders = new HashMap<String, DocumentBuilder>();
		lireFeatures = new HashMap<String, LireFeature>();
		featureUsed = conf.getInt(ConfUtils.GENERATEFEATUREVALUE, 0);
		freq = conf.getInt(ConfUtils.GENERATECRAWLFREQ, 1000);
		GenerateUtil.prepareFeaturesAndBuilders(featureNames, builders, lireFeatures);
		executor = Executors.newFixedThreadPool(1);
		reqConf = RequestConfig.custom().setSocketTimeout(TIMEOUT_SOCKET_MILLI).setConnectTimeout(TIMEOUT_CONNECT_MILLI).build();
	}
	 
	@Override
	protected void reduce(LongWritable key, Iterable<Text> vals, Context context) {
		
		for (Text val : vals) {
			String[] array = val.toString().split(ConfUtils.ID_HASH);
			if (array.length < 2) {
				continue;
			}
			final String url = StringUtils.trim(array[1]);
			Long id = Long.valueOf(StringUtils.trim(array[0]));
			response = null;
			try {
				Future<String> future = 
						executor.submit(new Callable<String>(){
							public String call() {
								try {
									return getFeature(url);
								} catch (ClientProtocolException e) {
									System.out.print(url + " illegal url");
								} catch (SocketTimeoutException e) {
									System.out.print(url + " socket times out");
								} catch (UnknownHostException e) {
									System.out.print(url + " unreachable host");	 
								} catch (ConnectTimeoutException e) {
									System.out.print(url + " refuses connection");
								} catch (FileNotFoundException e) {
									System.out.print(url + " is unavailable");
								} catch (SSLException e) {
									System.out.print(url + " is unavailable");
								} catch (SocketException e) {
									System.out.print(url + " socket resets");
								} catch (RasterFormatException e) {
									System.out.print(url + " image distorts");
								} catch (NoHttpResponseException e) {
									System.out.print(url + " no response");
								} catch (IOException e) {
									System.out.print(url + " IO exception");
								} catch (DecoderException e) {
									System.out.print(url + " not JPG");
								}
								return null;
						}});
				context.write(new LongWritable(id), new Text(future.get(TIMEOUT_TASK_SEC, TimeUnit.SECONDS)));
				Thread.sleep(freq);
			} catch(CancellationException e) {
				System.out.println(url + " is cancelled");
			} catch(ExecutionException e) { 
				System.out.println(url + " throws computation exception");
			} catch(TimeoutException e) {
				System.out.println(url + " times out");
			} catch (NullPointerException e) {// caused by raised exceptions from getFeature() method
				System.out.println(" , skipped"); 
			} catch(IOException e) {
					e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println(url + " is interrupted");
			} catch (CMMException e) {
				System.out.println(url + " has image damaged");
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (response != null) {
						response.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) {
		executor.shutdown();
	}
	
	private String getFeature(String url) throws ClientProtocolException, IOException, DecoderException {
		StringBuilder valBuilder = new StringBuilder();
		HttpGet httpget = new HttpGet(url);
		httpget.setConfig(reqConf);
		response = httpClient.execute(httpget);
		HttpEntity entity = response.getEntity();
		if (entity == null) {
			throw new ClientProtocolException("Response contains no content");
		}
		ContentType contentType = ContentType.get(entity);
		if (contentType != null && StringUtils.equalsIgnoreCase(contentType.getMimeType(), MIME_JPG)) {
			image = ImageIO.read(entity.getContent());
			if (image == null) {
				System.out.println(url + " cannot be found");
			}
			for (String featureName : builders.keySet()) {
				DocumentBuilder builder = builders.get(featureName);
				featureOutput = GenerateUtil.processImage(image, "",imageGrid, builder, featureName, featureOutput, lireFeatures, featureUsed);
				valBuilder.append(" " + ConfUtils.SEPARATOR + " " + featureName + " ");
				valBuilder.append(FormatUtil.toTextString(featureOutput));
			}			
		}
		return valBuilder.toString();
	}
	
}
