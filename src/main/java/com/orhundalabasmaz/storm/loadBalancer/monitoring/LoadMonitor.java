package com.orhundalabasmaz.storm.loadBalancer.monitoring;

import com.orhundalabasmaz.storm.loadBalancer.Configuration;
import com.orhundalabasmaz.storm.utils.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class LoadMonitor implements Serializable {
	private static long INFO_COUNT = 0;
	private int boltCount = 0;
	private final int N_BOLTS = Configuration.N_WORKER_BOLTS;  // N_WORKER_BOLTS
	private final String NL = "\n";

	private Map<String, Map<String, Integer>> valueLoadMap = new HashMap<>();
	private Map<String, Integer> relativeLoadMap = new HashMap<>(N_BOLTS);
	private Map<String, Map<String, Integer>> boltLoadMap = new HashMap<>(N_BOLTS);

	//	synchronized
	public void load(String boltId, Map<String, Integer> countMap) {
		if (!Configuration.LOG_ENABLED) {
			return;
		}
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

	private void init() {
		boltCount = 0;
	}

	/**
	 * distributed bolts for each key
	 * k1 -> {b1: 234, b2: 324, b3: 123}
	 */
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

	/**
	 * total key consumed within each bolt
	 * b1 -> 124
	 */
	private void handleRelativeBoltLoad(String boltId, Integer value) {
		Integer load = relativeLoadMap.get(boltId);
		if (load == null) {
			load = 0;
		}
		load += value;
		relativeLoadMap.put(boltId, load);
	}

	/**
	 * key based load for each bolt
	 * b1 -> {k1: 12, k2: 34, k3: 54}
	 */
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
//		printLoadInfoHeader();
//		printRelativeBoltLoadInfo();
//		printBoltLoadInfo();
//		printValueLoadInfo();
		printMemoryConsumptionInfo();
	}

	private void printLoadInfoHeader() {
		Logger.log(
				NL + NL + NL + NL + NL +
						"#############################" + NL +
						"### PRINTING LOAD INFO: #" + INFO_COUNT++ + NL +
						"#############################");
	}

	private void printRelativeBoltLoadInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append(NL)
				.append("##############################################").append(NL)
				.append("## RELATIVE LOAD ON BOLTS  (cumulative)     ##").append(NL);
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
					.append(NL);
		}
		Logger.log(sb.toString());
	}

	private void printBoltLoadInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append(NL)
				.append("#####################################").append(NL)
				.append("## LOAD BY BOLTS  (cumulative)     ##").append(NL);
		for (Map.Entry<String, Map<String, Integer>> entry : boltLoadMap.entrySet()) {
			String boltId = entry.getKey();
			Map<String, Integer> valueMap = entry.getValue();
			int totalCount = 0;
			for (Map.Entry<String, Integer> valueEntry : valueMap.entrySet()) {
				Integer count = valueEntry.getValue();
				totalCount += count;
			}
			sb.append("bolt: ").append(boltId).append(NL);
			for (Map.Entry<String, Integer> valueEntry : valueMap.entrySet()) {
				String value = valueEntry.getKey();
				Integer count = valueEntry.getValue();
				double load = (double) count / totalCount;
				String loadPercent = String.format("%.2f", load * 100);
				sb.append("\t")
						.append(value).append(" ")
						.append("%").append(loadPercent)
						.append(" [").append(count).append("/").append(totalCount).append("]")
						.append(NL);
			}
		}
		Logger.log(sb.toString());
	}

	private void printValueLoadInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append(NL)
				.append("##############################").append(NL)
				.append("## LOAD BY KEY (cumulative) ##").append(NL);
		for (Map.Entry<String, Map<String, Integer>> loadEntry : valueLoadMap.entrySet()) {
			String key = loadEntry.getKey();
			Map<String, Integer> boltMap = loadEntry.getValue();
			int totalCount = 0;
			for (Map.Entry<String, Integer> boltEntry : boltMap.entrySet()) {
				Integer count = boltEntry.getValue();
				totalCount += count;
			}
			sb.append("# ").append(key).append("\t[AGG: ").append(boltMap.size()).append("]").append(NL);
			for (Map.Entry<String, Integer> boltEntry : boltMap.entrySet()) {
				String boltId = boltEntry.getKey();
				Integer count = boltEntry.getValue();
				double load = (double) count / totalCount;
				String loadPercent = String.format("%.2f", load * 100);
				sb.append("\t").append("bolt: ").append(boltId)
						.append(" > %").append(loadPercent)
						.append(" [").append(count).append("/").append(totalCount).append("]")
						.append(NL);
			}
		}
		Logger.log(sb.toString());
	}

	private void printMemoryConsumptionInfo() {
		String consumption = NL +
				"##############################################" + NL +
				"## MEMORY CONSUMPTION                       ##" + NL +
				getMemoryConsumptionInfo() + NL;
		Logger.log(consumption);
	}

	public String getMemoryConsumptionInfo() {
		int NK = valueLoadMap.size();
		int NT = 0;
		for (Map<String, Integer> keys : boltLoadMap.values()) {
			NT += keys.size();
		}
		double MC = NK > 0 ? (double) NT / NK : 0;
		return "NK: " + NK +
				", NT: " + NT + " >> " +
				"Memory Consumption ratio is " + String.format("%.2f", MC);
	}
}
