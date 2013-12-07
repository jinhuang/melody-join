package com.iojin.melody.bsp;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;

import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.DualBound;
import com.iojin.melody.utils.FileUtil;
import com.iojin.melody.utils.FormatUtil;
import com.iojin.melody.utils.Grid;
import com.iojin.melody.utils.HistUtil;
import com.iojin.melody.utils.QuantileGrid;
import com.iojin.melody.utils.TimerUtil;

public class Normal {
	
	private static int numInterval = 5;
	private static int lengthError = 2 * numInterval + 1;
	private static int numDuals = 10;
	private static int numGrid = 2;
	private static Percentile percentile = new Percentile();
	private static Random random = new Random();
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		if (args.length != 11) {
			System.out.println("USAGE: <NUM_TASK> <PARAK> <DIMENSION> <NUM_BIN> <NUM_VECTOR> <INPUT_PATH> <BIN_PATH> <VECTOR_PATH> <OUTPUT_PATH> <CACHED> <BATCH>");
			return;
		}
		HamaConfiguration conf = new HamaConfiguration();
		conf.set("mapred.child.java.opts", "-Xmx512M");
		Path out = new Path(args[8]);
		FileSystem fs = FileSystem.get(conf);
		
		FileUtil.deleteIfExistOnHDFS(conf, args[8]);
		FileUtil.addDependency(conf);
		DistributedCache.addCacheFile(new URI(args[6]),conf);
		DistributedCache.addCacheFile(new URI(args[7]),conf);
		
		conf.setInt(NormalBSP.NUMTASK, Integer.valueOf(args[0]));
		conf.setInt(NormalBSP.PARAK, Integer.valueOf(args[1]));
		conf.setInt(NormalBSP.DIMENSION, Integer.valueOf(args[2]));
		conf.setInt(NormalBSP.NUMBIN, Integer.valueOf(args[3]));
		conf.setInt(NormalBSP.NUMVEC, Integer.valueOf(args[4]));
		conf.setInt(NormalBSP.NUMINTERVAL, numInterval);
		conf.setInt(NormalBSP.NUMDUAL, numDuals);
		conf.setInt(NormalBSP.NUMGRID, numGrid);
		conf.setInt(NormalBSP.LENGTHERROR, lengthError);
		conf.set(NormalBSP.PATHIN, args[5]);
		conf.set(NormalBSP.PATHPREIN, args[5] + File.separator + "prepared");
		Path in = new Path(conf.get(NormalBSP.PATHPREIN));
		if (fs.isFile(in)) {
			System.out.println("Input should be a directory");
			return;
		}
		

		conf.set(NormalBSP.PATHCELL, args[5] + File.separator + "cell");
		conf.set(NormalBSP.PATHGRID, args[5] + File.separator + "grid");
		conf.set(NormalBSP.PATHBIN, args[6]);
		conf.set(NormalBSP.PATHVEC, args[7]);
		conf.set(NormalBSP.PATHOUT, args[8]);
		conf.setBoolean(NormalBSP.CACHED, Boolean.valueOf(args[9]));
		conf.setInt(NormalBSP.MSG_BATCH, Integer.valueOf(args[10]));
		conf.set("bsp.local.tasks.maximum", "" + Runtime.getRuntime().availableProcessors());
		
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHPREIN));
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHCELL));
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHGRID));
		
		BSPJob job = NormalBSP.createJob(conf, in, out);
		job.setNumBspTask(Integer.valueOf(args[0]));
		TimerUtil.start();
		prepareInput(conf);
		job.waitForCompletion(true);
		TimerUtil.end();
		TimerUtil.print();
		
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHPREIN));
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHCELL));
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHGRID));
	}
	
	private static void prepareInput(HamaConfiguration conf) throws IOException {
		List<Double[]> histograms = FileUtil.readMultiFromHDFS(conf, conf.get(NormalBSP.PATHIN));
		double[] bins = FormatUtil.toDoubleArray(FileUtil.readSingleFromHDFS(conf, conf.get(NormalBSP.PATHBIN)));
		double[] allVectors = FormatUtil.toDoubleArray(FileUtil.readSingleFromHDFS(conf, conf.get(NormalBSP.PATHVEC)));
		List<Double[]> vectors = new ArrayList<Double[]>();
		int dimension = conf.getInt(NormalBSP.DIMENSION, 3);
		int numBins = conf.getInt(NormalBSP.NUMBIN, 0);
		int numVectors = conf.getInt(NormalBSP.NUMVEC, 0);
		for (int i = 0; i < numVectors; i++) {
			vectors.add(FormatUtil.toObjectDoubleArray(FormatUtil.getNthSubArray(allVectors, dimension, i)));
		}
		
		double[][] ms = new double[numVectors][histograms.size()];
		double[][] bs = new double[numVectors][histograms.size()];
		double[][] sws = new double[numVectors][histograms.size()];
		double[][] ses = new double[numVectors][histograms.size()];
		int c = 0;
		double[][] projectedBins = new double[numVectors][numBins];
		double[] tmin = new double[numVectors];
		double[] tmax = new double[numVectors];
		for (int v = 0; v < numVectors; v++) {
			projectedBins[v] = FormatUtil.substractAvg(HistUtil.projectBins(bins, dimension, FormatUtil.toDoubleArray(vectors.get(v))));
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
		HashMap<String, Integer> cellCount = new HashMap<String, Integer>();
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
		
		HashMap<String, Integer> assignment = Grid.assignGridInt(cellCount, conf.getInt(NormalBSP.NUMTASK, 1));
		HashMap<Integer, List<String>> workload = new HashMap<Integer, List<String>>();
		for (String each : assignment.keySet()) {
			if (!workload.containsKey(assignment.get(each))) {
				workload.put(assignment.get(each), new ArrayList<String>());
			}
			workload.get(assignment.get(each)).add(each);
		}
		
		// write out
		FileSystem fs = FileSystem.get(conf);
		// data
		for(Integer id : workload.keySet()) {
			String path = conf.get(NormalBSP.PATHPREIN) + File.separator + Long.toHexString(Double.doubleToLongBits(Math.random()));
			FileUtil.deleteIfExistOnHDFS(conf, path);
			FSDataOutputStream out = fs.create(new Path(path));
//			out.writeBytes(id + "\n");
			for (String cell : workload.get(id)) {
//				out.writeBytes(cell + "\n");
				for (Integer each : cellIds.get(cell)) {
					out.writeBytes(id + ";" + cell + ";" + FormatUtil.toTextString(histograms.get(each)) + "\n");
				}				
			}
			out.flush();
			out.close();
		}
		// cell error, rubner, dual
		FileUtil.deleteIfExistOnHDFS(conf, conf.get(NormalBSP.PATHCELL));
		FSDataOutputStream cellOut = fs.create(new Path(conf.get(NormalBSP.PATHCELL)));
		for(String cell : cellError.keySet()) {
			cellOut.writeBytes(cell + ";" + assignment.get(cell) + ";");
			for (Double[] eachError : cellError.get(cell)) {
				cellOut.writeBytes(FormatUtil.toTextString(eachError) + ";");
			}
			cellOut.writeBytes(FormatUtil.toTextString(cellRubner.get(cell)) + ";");
			cellOut.writeBytes(FormatUtil.toTextString(cellDual.get(cell)) + "\n");
		}
		cellOut.flush();
		cellOut.close();
		// grid
		FSDataOutputStream gridOut = fs.create(new Path(conf.get(NormalBSP.PATHGRID)));
		for(int i = 0; i < numVectors; i++) {
			gridOut.writeBytes(i + ";" + FormatUtil.toTextString(domain[i]) + ";" + 
					FormatUtil.toTextString(slope[i]) + ";" + 
					FormatUtil.toTextString(swQ[i]) + ";" + 
					FormatUtil.toTextString(seQ[i]) + "\n");
		}
		gridOut.flush();
		gridOut.close();
	}
}
