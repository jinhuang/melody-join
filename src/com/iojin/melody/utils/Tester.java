package com.iojin.melody.utils;

//import java.io.File;
//import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
//import java.util.Random;
import java.util.TreeMap;

//import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class Tester {
	
	private static int numBins = 32;
	private static int numVectors = 3;
	private static int numDuals = 10;
	private static int dimension = 3;
	private static int numInterval = 5;
	private static int lengthError = 2 * numInterval + 1;
	private static int numGrid = 4;
	private static Percentile percentile = new Percentile();
	private static Random random = new Random();
	
	private static List<Double[]> readHistograms() throws IOException {
		List<Double[]> histograms = new ArrayList<Double[]>();
		List<Double[]> temp1 = FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/synthetic_extension/128/out0");
//		List<Double[]> temp2 = FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/mirflickr_extension/6x100_horizontal/normal1");
//		List<Double[]> temp3 = FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/mirflickr_extension/6x100_horizontal/normal2");
//		List<Double[]> temp4 = FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/mirflickr_extension/6x100_horizontal/normal3");
//		List<Double[]> temp5 = FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/mirflickr_extension/6x100_horizontal/normal4");
//		List<Double[]> temp6 = FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/mirflickr_extension/6x100_horizontal/normal5");
		histograms.addAll(temp1);
//		histograms.addAll(temp2);
//		histograms.addAll(temp3);
//		histograms.addAll(temp4);
//		histograms.addAll(temp5);
//		histograms.addAll(temp6);
		return histograms;
	}
	
	private static double[] readBins() throws IOException {
		return FormatUtil.toDoubleArray(FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/synthetic_extension/bins.txt").get(0));
	}
	
	private static List<Double[]> readVectors() throws IOException {
		return FileUtil.readArraysLocalFile("/home/soone/workspace/archive/emd/emd_data/synthetic_extension/vector_10_sl.txt");
	}
	
	public static void testEmd() throws IOException {
		List<Double[]> histograms = readHistograms();
		double[] bins = readBins();
		double[] emds = new double[histograms.size() * (histograms.size() - 1) / 2 ];
		int counter = 0;
		for (int i = 0; i < histograms.size(); i++) {
			double [] histA = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(i)), 1, numBins));
			for (int j = i + 1; j < histograms.size(); j++) {
				double [] histB = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(j)), 1, numBins));
				emds[counter] = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
				counter++;
			}
		}
		percentile.setData(emds);
		System.out.println("emd distribution: ");
		System.out.println("0% : " + percentile.evaluate(0.001));
		for (int i = 1; i <= 20; i++) {
			System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
		}
	}
	
	private static void testFramework() {
		try {
			List<Double[]> histograms = readHistograms();
			double[] bins = readBins();
			List<Double[]> vectors = readVectors();
			
			double[][] ms = new double[numVectors][histograms.size()];
			double[][] bs = new double[numVectors][histograms.size()];
			double[][] sws = new double[numVectors][histograms.size()];
			double[][] ses = new double[numVectors][histograms.size()];
			int c = 0;
			double[][] projectedBins = new double[numVectors][numBins];
			double[] tmin = new double[numVectors];
			double[] tmax = new double[numVectors];
			for (int v = 0; v < numVectors; v++) {
				projectedBins[v] = HistUtil.substractAvg(HistUtil.projectBins(bins, dimension, FormatUtil.toDoubleArray(vectors.get(v))));
				tmin[v] = HistUtil.getMinIn(projectedBins[v]);
				tmax[v] = HistUtil.getMaxIn(projectedBins[v]);
			}

			double [][] weight = new double[histograms.size()][numBins];
			double [][] allRubner = new double[histograms.size()][dimension];
			double [][][] error = new double[numVectors][histograms.size()][lengthError];
 			// build a grid
			for (Double[] each : histograms) {
				double [] weights = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(each), 1, numBins));
				weight[c] = weights;
				for (int v = 0; v < numVectors; v++) {
					NormalDistributionImpl normal = HistUtil.getNormal(weights, projectedBins[v]);
					TreeMap<Double, Double> cdf = HistUtil.getDiscreteCDFNormalized(weights, projectedBins[v]);	
					List<Double> errors = HistUtil.getMinMaxError(normal, cdf, numInterval);
					for (int i = 0; i < errors.size(); i++) {
						error[v][c][i] = errors.get(i);
					}
					error[v][c][lengthError - 1] = HistUtil.getFullError(normal, cdf, tmin[v], tmax[v]);
					ms[v][c] = 1 / normal.getStandardDeviation();
					bs[v][c] = (-1) * normal.getMean() / normal.getStandardDeviation();
				}
				c++;
			}
			double[][] domain = new double[numVectors][4];
			double[][] slope = new double[numVectors][2];
			Grid[] grid = new Grid[numVectors];
			for (int v = 0; v < numVectors; v++) {
				domain[v][0] = HistUtil.getMinIn(ms[v]);
				domain[v][1] = HistUtil.getMaxIn(ms[v]);
				domain[v][2] = HistUtil.getMinIn(bs[v]);
				domain[v][3] = HistUtil.getMaxIn(bs[v]);
				slope[v][0] = -tmax[v];
				slope[v][1] = -tmin[v];
				grid[v] = new Grid(domain[v], slope[v], numGrid);
			}
			
			DualBound[] duals = new DualBound[numDuals];
			for (int i = 0; i < numDuals; i++) {
				int pickA = random.nextInt(weight.length);
				int pickB = random.nextInt(weight.length);
				while(pickB == pickA) pickB = random.nextInt(weight.length);
				duals[i] = new DualBound(weight[pickA], weight[pickB], bins, dimension);
			}
			
			// get projections and build quantile grid
			double [] pt = new double[2];
			for (int i = 0; i < c; i++) {
				for (int v = 0; v < numVectors; v++) {
					pt[0] = ms[v][i];
					pt[1] = bs[v][i];
					double[] dist = grid[v].getProjectionDistanceInGrid(pt);
					sws[v][i] = dist[0];
					ses[v][i] = dist[1];
				}
			}
			double[][] swQ = new double[numVectors][numGrid + 1];
			double[][] seQ = new double[numVectors][numGrid + 1];
			QuantileGrid[] qGrid = new QuantileGrid[numVectors];
			for (int v = 0; v < numVectors; v++) {
				percentile.setData(sws[v]);
				swQ[v][0] = percentile.evaluate(0.001);
				for (int i = 1; i <= numGrid; i++) {
					swQ[v][i] = percentile.evaluate(i * 100.0 / numGrid);
				}		
				percentile.setData(ses[v]);
				seQ[v][0] = percentile.evaluate(0.001);
				for (int i = 1; i <= numGrid; i++) {
					seQ[v][i] = percentile.evaluate(i * 100.0 / numGrid);
				}
				qGrid[v] = new QuantileGrid(domain[v], slope[v], numGrid, swQ[v], seQ[v]);
			}
			
			// aggregate errors
			Map<String, Double[][]> cellError = new HashMap<String, Double[][]>();
			Map<String, Integer> cellCount = new HashMap<String, Integer>();
			Map<String, List<Integer>> cellIds = new HashMap<String, List<Integer>>();
			Map<String, Double[]> cellRubner = new HashMap<String, Double[]>();
			Map<String, Double[]> cellDual = new HashMap<String, Double[]>();
			double[] record = new double[2];
			for (int i = 0; i < c; i++) {
				String id = "";
				double[] weights = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(i)), 1, numBins));
				double[] rubnerValue = DistanceUtil.getRubnerValue(weights, dimension, bins);
				double[] dualValue = new double[numDuals];
				for (int m = 0; m < numDuals; m++) {
					dualValue[m] = duals[m].getKey(weights);
				}
				allRubner[i] = rubnerValue;
				for (int v = 0; v < numVectors; v++) {
					record[0] = ms[v][i];
					record[1] = bs[v][i];
					id += String.valueOf(qGrid[v].getGridId(record)) + ",";
				}
				if (!cellError.containsKey(id)) {
					Double[][] each = new Double[numVectors][lengthError + 1];
					for (int v = 0; v < numVectors; v++) {
						each[v] = new Double[lengthError + 1];
						for (int j = 0; j < lengthError + 1; j++) {
							if (j%2 == 0) {
								each[v][j] = Double.MAX_VALUE;
							}
							else {
								each[v][j]= -Double.MAX_VALUE;
							}
						}
					}
					cellError.put(id, each);
				}
				if (!cellCount.containsKey(id)) {
					cellCount.put(id, 0);
				}
				if (!cellIds.containsKey(id)) {
					List<Integer> temp = new ArrayList<Integer>();
					temp.add(i);
					cellIds.put(id, temp);
				}
				else {
					cellIds.get(id).add(i);
				}
				if (!cellRubner.containsKey(id)) {
					Double[] rubner = new Double[dimension * 2];
					for (int m = 0; m < dimension; m++) {
						rubner[m] = Double.MAX_VALUE;
						rubner[dimension + m] = -Double.MAX_VALUE;
					}
					cellRubner.put(id, rubner);
				}
				if (!cellDual.containsKey(id)) {
					Double[] dual = new Double[numDuals * 2];
					for (int m = 0; m < numDuals; m++) {
						dual[m] = Double.MAX_VALUE;
						dual[m + numDuals] = -Double.MAX_VALUE;
					}
					cellDual.put(id, dual);
				}
				
				cellCount.put(id, cellCount.get(id) + 1);
				Double[][] each = cellError.get(id);
				for (int v = 0; v < numVectors; v++) {
					for (int j = 0; j < lengthError - 1; j++) {
						if (j % 2 == 0) {
							each[v][j] = each[v][j] < error[v][i][j] ? each[v][j] : error[v][i][j];
						}
						else {
							each[v][j] = each[v][j] > error[v][i][j] ? each[v][j] : error[v][i][j];
						}
					}
					each[v][lengthError - 1] = each[v][lengthError - 1] < error[v][i][lengthError - 1] ? each[v][lengthError - 1] : error[v][i][lengthError - 1];
					each[v][lengthError] = each[v][lengthError] > error[v][i][lengthError - 1] ? each[v][lengthError] : error[v][i][lengthError - 1];
				}
				cellError.put(id, each);
				
				Double[] eachRubner = cellRubner.get(id);
				for (int m = 0; m < dimension; m++) {
					eachRubner[m] = eachRubner[m] < rubnerValue[m] ? eachRubner[m] : rubnerValue[m];
					eachRubner[m + dimension] = eachRubner[m + dimension] > rubnerValue[m] ? eachRubner[m + dimension] : rubnerValue[m];
				}
				cellRubner.put(id, eachRubner);
				
				Double[] eachDual = cellDual.get(id);
				for (int m = 0; m < numDuals; m++) {
					eachDual[m] = eachDual[m] < dualValue[m] ? eachDual[m] : dualValue[m];
					eachDual[m + numDuals] = eachDual[m + numDuals] > dualValue[m] ? eachDual[m + numDuals] : dualValue[m];
				}
				cellDual.put(id, eachDual);
			}
			
			// test emdbr
			List<Double> ratio = new ArrayList<Double> ();
			List<Double> exactRatio = new ArrayList<Double> ();
			List<Double> rubnerRatio = new ArrayList<Double>();
			List<Double> exactRubnerRatio = new ArrayList<Double> ();
			List<Double> rubnerEmdCollection = new ArrayList<Double>();
			List<Double> exactEmdCollection = new ArrayList<Double>();
			List<Double> maxEmdCollection = new ArrayList<Double>();
//			double[] compRecord = new double[2];
			for (int i = 0; i < c; i++) {
				String id = "";
				for (int v = 0; v < numVectors; v++) {
					record[0] = ms[v][i];
					record[1] = bs[v][i];
					id += String.valueOf(qGrid[v].getGridId(record)) + ",";
				}
				for (String cellId : cellCount.keySet()) {
					if (cellId.equals(id)) continue;
					String[] eachCellIds = cellId.split(",");
					double maxEmdBr = -Double.MAX_VALUE;
					for (int v = 0; v < numVectors; v++) {
						record[0] = ms[v][i];
						record[1] = bs[v][i];
						int eachId = Integer.valueOf(eachCellIds[v]);
						double[] bound = qGrid[v].getGridBound(eachId);
						Direction direction = qGrid[v].locateRecordToGrid(record, bound);
						double emdBr = qGrid[v].getEmdBr(record, error[v][i], bound, FormatUtil.toDoubleArray(cellError.get(id)[v]), direction,numInterval);
						maxEmdBr = maxEmdBr > emdBr ? maxEmdBr : emdBr;
					}
					//double[] rubnerValue = DistanceUtil.getRubnerValue(weight[i], dimension, bins);
					double[] rubnerValue = allRubner[i];
					double rubnerEmd = DistanceUtil.getRubnerBound(rubnerValue, FormatUtil.toDoubleArray(cellRubner.get(cellId)), dimension);
					rubnerEmdCollection.add(rubnerEmd);
					double maxEmdNormal = Double.MAX_VALUE;
					double minRubnerEmd = Double.MAX_VALUE;
					double minExactEmd = Double.MAX_VALUE;
					double maxEmd = -Double.MAX_VALUE;
					
					List<Integer> temp = cellIds.get(cellId);
					for (Integer eachId : temp) {
						for (int v = 0; v < numVectors; v++) {
							double normalEmd = HistUtil.getNormalEmd(weight[i], weight[eachId], projectedBins[v], numInterval);
							maxEmdNormal = maxEmdNormal < normalEmd ? maxEmdNormal : normalEmd;
						}
						double each = DistanceUtil.getRubnerEmd(weight[i], weight[eachId], dimension, bins, DistanceType.LTWO);
						minRubnerEmd = minRubnerEmd < each ? minRubnerEmd : each;	
						
						double emd = DistanceUtil.getEmdLTwo(weight[i], weight[eachId], dimension, bins);
						exactEmdCollection.add(emd);
						
						
						minExactEmd = minExactEmd < emd ? minExactEmd : emd;
						maxEmd = maxEmd > emd ? maxEmd : emd;
					}
					
					if (Math.abs(maxEmdNormal) < 0.00001) {
						ratio.add(1.0);
					}
					else {
						ratio.add(maxEmdBr / maxEmdNormal);
					}
					if (Math.abs(minRubnerEmd) < 0.00001) {
						ratio.add(1.0);
					}
					else {
//						if (rubnerEmd / minRubnerEmd > 1) {
//							int debug = 0;
//							debug = debug * debug;
//						}
						rubnerRatio.add(rubnerEmd / minRubnerEmd);
					}
					exactRatio.add(maxEmdBr / minExactEmd);
					exactRubnerRatio.add(rubnerEmd / minExactEmd);
					if (rubnerEmd / minExactEmd > 1) {
						int debug = 0;
						debug = debug * debug;
					}
					maxEmdCollection.add(maxEmd);
				}
				
			}
			
			double[] ratioarray = new double[rubnerRatio.size()];
			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = rubnerRatio.get(i);
			percentile.setData(ratioarray);
			System.out.println("rubner: ");
			System.out.println("0% : " + percentile.evaluate(0.001));
			for (int i = 1; i <= 20; i++) {
				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
			}
//			
//			ratioarray = new double[ratio.size()];
//			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = ratio.get(i);
//			percentile.setData(ratioarray);
//			System.out.println("normal: ");
//			System.out.println("0% : " + percentile.evaluate(0.001));
//			for (int i = 1; i <= 20; i++) {
//				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
//			}
			
			ratioarray = new double[exactRubnerRatio.size()];
			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = exactRubnerRatio.get(i);
			percentile.setData(ratioarray);
			System.out.println("rubner: ");
			System.out.println("0% : " + percentile.evaluate(0.001));
			for (int i = 1; i <= 20; i++) {
				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
			}
			
//			ratioarray = new double[exactRatio.size()];
//			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = exactRatio.get(i);
//			percentile.setData(ratioarray);
//			System.out.println("normal: ");
//			System.out.println("0% : " + percentile.evaluate(0.001));
//			for (int i = 1; i <= 20; i++) {
//				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
//			}
			
//			ratioarray = new double[rubnerEmdCollection.size()];
//			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = rubnerEmdCollection.get(i);
//			percentile.setData(ratioarray);
//			System.out.println("distribution: ");
//			System.out.println("0% : " + percentile.evaluate(0.001));
//			for (int i = 1; i <= 20; i++) {
//				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
//			}
			
//			ratioarray = new double[exactEmdCollection.size()];
//			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = exactEmdCollection.get(i);
//			percentile.setData(ratioarray);
//			System.out.println("emd distribution: ");
//			System.out.println("0% : " + percentile.evaluate(0.001));
//			for (int i = 1; i <= 20; i++) {
//				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
//			}
			
//			ratioarray = new double[maxEmdCollection.size()];
//			for (int i = 0; i < ratioarray.length; i++) ratioarray[i] = maxEmdCollection.get(i);
//			percentile.setData(ratioarray);
//			System.out.println("max emd distribution: ");
//			System.out.println("0% : " + percentile.evaluate(0.001));
//			for (int i = 1; i <= 20; i++) {
//				System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
//			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public static void testFlow() throws IOException {
		List<Double[]> histograms = readHistograms();
		double[] bins = readBins();		
		int[] pick = new int[10];
		for (int i = 0; i < pick.length; i++) {
			pick[i] = random.nextInt(histograms.size());
		}
		int c = 0;
		double[] flowRatio = new double[histograms.size() * (histograms.size() - 1) / 2];
		for (int i = 0; i < histograms.size(); i++) {
			double [] histA = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(i)), 1, numBins));
			for (int j = i + 1; j < histograms.size(); j++) {
				double [] histB = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(j)), 1, numBins));
				
				double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
				double flow = Double.MAX_VALUE;
				for (int k = 0; k < pick.length; k++) {
					double temp = 0.0;
					temp += HistUtil.getFlowBetween(histA, FormatUtil.toObjectDoubleArray(HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(pick[k])), 1, numBins))), bins, dimension);
					temp += HistUtil.getFlowBetween(histB, FormatUtil.toObjectDoubleArray(HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(pick[k])), 1, numBins))), bins, dimension);
					flow = temp > flow ? flow : temp;
				}
				flowRatio[c] = flow / emd;
				c++;
			}
		}
		percentile.setData(flowRatio);
		System.out.println("0% : " + percentile.evaluate(0.001));
		for (int i = 1; i <= 20; i++) {
			System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
		}		
	}
	

	
	public static void testEmdBound() throws IOException {
		List<Double[]> histograms = readHistograms();
		double[] bins = readBins();
//		List<Double[]> vectors = readVectors();		
//		double [] projectedBins = FormatUtil.substractAvg(HistUtil.projectBins(bins, dimension, FormatUtil.toDoubleArray(vectors.get(0))));
		double[] boundRatio = new double[histograms.size() * (histograms.size() - 1) / 2];
		int counter = 0;
		
		Random random = new Random();
		int pickA = random.nextInt(histograms.size());
		int pickB = random.nextInt(histograms.size());
		while(pickA == pickB) pickB = random.nextInt(histograms.size());
//		double[] pickHistA = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(pickA)), 1, numBins));
//		double[] pickHistB = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(pickB)), 1, numBins));
//		DualBound dualBound = new DualBound(pickHistA, pickHistB, bins, dimension);
		
		for (int i = 0; i < histograms.size(); i++) {
			double [] histA = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(i)), 1, numBins));
			for (int j = i + 1; j < histograms.size(); j++) {
				double [] histB = HistUtil.normalizeArray(FormatUtil.getSubArray(FormatUtil.toDoubleArray(histograms.get(j)), 1, numBins));
//				double projectEmd = HistUtil.getProjectEmd(histA, histB, projectedBins);
//				double dualEmd = DistanceUtil.getIndMinEmd(histA, histB, dimension, bins, DistanceType.LONE, null);
				double rubnerEmd = DistanceUtil.getRubnerEmd(histA, histB, dimension, bins, DistanceType.LTWO);
				double emd = DistanceUtil.getEmdLTwo(histA, histB, dimension, bins);
				boundRatio[counter] = rubnerEmd / emd;
				counter++;
			}
		}
		percentile.setData(boundRatio);
		System.out.println("0% : " + percentile.evaluate(0.001));
		for (int i = 1; i <= 20; i++) {
			System.out.println(i* 5 + "% : " + percentile.evaluate(i * 5));
		}		
	}
	
	public static void main(String[] args) throws IOException {
		testFramework();
//		testEmdBound();
//		testFlow();
//		testEmd();
	}		
}
