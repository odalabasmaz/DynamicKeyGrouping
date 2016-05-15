package com.orhundalabasmaz.storm.loadBalancer.monitoring;

import com.orhundalabasmaz.storm.loadBalancer.Configuration;
import com.orhundalabasmaz.storm.utils.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class LoadMonitor {

	private static long INFO_COUNT = 0;
	private int boltCount = 0;
	private final int N_BOLTS = Configuration.N_COUNTER_BOLTS;  // N_COUNTER_BOLTS

	private Map<String, Map<String, Integer>> valueLoadMap = new HashMap<>();
	private Map<String, Integer> relativeLoadMap = new HashMap<>(N_BOLTS);
	private Map<String, Map<String, Integer>> boltLoadMap = new HashMap<>(N_BOLTS);

	synchronized
	public void load(String boltId, Map<String, Integer> countMap) {
		for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();

			// runs as cumulative !!
			handleValueLoad(boltId, key, value);
			handleRelativeBoltLoad(boltId, value);
			handleBoltLoad(boltId, key, value);
		}

		if (++boltCount == N_BOLTS) {
			printLoadInfo();
			init();
		}
	}

	private void handleValueLoad(String boltId, String key, Integer value) {
		Map<String, Integer> boltMap = valueLoadMap.get(key);
		if (boltMap == null) {
			boltMap = new HashMap<>();
			valueLoadMap.put(key, boltMap);
		}
		Integer count = boltMap.get(boltId);
		if (count == null) {
			count = 0;
		}
		boltMap.put(boltId, count + value);
	}

	private void handleRelativeBoltLoad(String boltId, Integer value) {
		Integer load = relativeLoadMap.get(boltId);
		if (load == null) {
			load = 0;
		}
		load += value;
		relativeLoadMap.put(boltId, load);
	}

	private void handleBoltLoad(String boltId, String key, Integer value) {
		Map<String, Integer> valueMap = boltLoadMap.get(boltId);
		if (valueMap == null) {
			valueMap = new HashMap<>();
			boltLoadMap.put(boltId, valueMap);
		}
		Integer count = valueMap.get(key);
		if (count == null) {
			count = 0;
		}
		count += value;
		valueMap.put(key, count);
	}

	private void printLoadInfo() {
		Logger.log(
				"\n\n\n\n\n" +
						"#############################" + "\n" +
						"### PRINTING LOAD INFO: #" + INFO_COUNT++ + "\n" +
						"#############################");
		printRelativeBoltLoadInfo();
		printBoltLoadInfo();
		printValueLoadInfo();
	}

	private void printRelativeBoltLoadInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("##############################################").append("\n")
				.append("## RELATIVE LOAD ON BOLTS  (cumulative)     ##").append("\n");
		int totalLoad = 0;
		for (Map.Entry<String, Integer> entry : relativeLoadMap.entrySet()) {
			Integer load = entry.getValue();
			totalLoad += load;
		}
		for (Map.Entry<String, Integer> entry : relativeLoadMap.entrySet()) {
			String boltId = entry.getKey();
			Integer load = entry.getValue();
			double relativeLoad = (double) load / totalLoad;
			String loadPercent = String.format("%.2f", relativeLoad * 100);
			sb.append("bolt: ").append(boltId)
					.append(" >> %").append(loadPercent).append(" ")
					.append(" [").append(load).append("/").append(totalLoad).append("]")
					.append("\n");
		}
		Logger.log(sb.toString());
	}

	private void printBoltLoadInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("#####################################").append("\n")
				.append("## LOAD BY BOLTS  (cumulative)     ##").append("\n");
		for (Map.Entry<String, Map<String, Integer>> entry : boltLoadMap.entrySet()) {
			String boltId = entry.getKey();
			Map<String, Integer> valueMap = entry.getValue();
			int totalCount = 0;
			for (Map.Entry<String, Integer> valueEntry : valueMap.entrySet()) {
				Integer count = valueEntry.getValue();
				totalCount += count;
			}
			sb.append("bolt: ").append(boltId).append("\n");
			for (Map.Entry<String, Integer> valueEntry : valueMap.entrySet()) {
				String value = valueEntry.getKey();
				Integer count = valueEntry.getValue();
				double load = (double) count / totalCount;
				String loadPercent = String.format("%.2f", load * 100);
				sb.append("\t")
						.append(value).append(" ")
						.append("%").append(loadPercent)
						.append(" [").append(count).append("/").append(totalCount).append("]")
						.append("\n");
			}
		}
		Logger.log(sb.toString());
	}

	private void printValueLoadInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("##############################").append("\n")
				.append("## LOAD BY KEY (cumulative) ##").append("\n");
		for (Map.Entry<String, Map<String, Integer>> loadEntry : valueLoadMap.entrySet()) {
			String key = loadEntry.getKey();
			Map<String, Integer> boltMap = loadEntry.getValue();
			int totalCount = 0;
			for (Map.Entry<String, Integer> boltEntry : boltMap.entrySet()) {
				Integer count = boltEntry.getValue();
				totalCount += count;
			}
			sb.append("# ").append(key).append("\t[AGG: ").append(boltMap.size()).append("]").append("\n");
			for (Map.Entry<String, Integer> boltEntry : boltMap.entrySet()) {
				String boltId = boltEntry.getKey();
				Integer count = boltEntry.getValue();
				double load = (double) count / totalCount;
				String loadPercent = String.format("%.2f", load * 100);
				sb.append("\t").append("bolt: ").append(boltId)
						.append(" > %").append(loadPercent)
						.append(" [").append(count).append("/").append(totalCount).append("]")
						.append("\n");
			}
		}
		Logger.log(sb.toString());
	}

	public void init() {
		boltCount = 0;
	}
}
