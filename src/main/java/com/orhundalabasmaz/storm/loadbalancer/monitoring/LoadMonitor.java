package com.orhundalabasmaz.storm.loadbalancer.monitoring;

import com.orhundalabasmaz.storm.utils.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class LoadMonitor implements Serializable {
	private static long INFO_COUNT = 0;
	private long boltCount = 0;
	private String NL = "\n";
	private boolean isLogEnabled;
	private int numberOfWorkerBolts;

	private Map<String, Map<String, Long>> valueLoadMap = new HashMap<>();
	private Map<String, Long> relativeLoadMap = new HashMap<>(numberOfWorkerBolts);
	private Map<String, Map<String, Long>> boltLoadMap = new HashMap<>(numberOfWorkerBolts);

	public LoadMonitor(boolean isLogEnabled, int numberOfWorkerBolts) {
		this.isLogEnabled = isLogEnabled;
		this.numberOfWorkerBolts = numberOfWorkerBolts;
	}

	//	synchronized
	public void load(String boltId, Map<String, Long> countMap) {
		if (!isLogEnabled) {
			return;
		}
		for (Map.Entry<String, Long> entry : countMap.entrySet()) {
			String key = entry.getKey();
			Long value = entry.getValue();

			// runs as cumulative !!
			handleValueLoad(boltId, key, value);
			handleRelativeBoltLoad(boltId, value);
			handleBoltLoad(boltId, key, value);
		}

		if (++boltCount == numberOfWorkerBolts) {
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
	private void handleValueLoad(String boltId, String key, Long value) {
		Map<String, Long> boltMap = valueLoadMap.get(key);
		if (boltMap == null) {
			boltMap = new HashMap<>();
			valueLoadMap.put(key, boltMap);
		}
		Long count = boltMap.get(boltId);
		if (count == null) {
			count = 0L;
		}
		boltMap.put(boltId, count + value);
	}

	/**
	 * total key consumed within each bolt
	 * b1 -> 124
	 */
	private void handleRelativeBoltLoad(String boltId, Long value) {
		Long load = relativeLoadMap.get(boltId);
		if (load == null) {
			load = 0L;
		}
		load += value;
		relativeLoadMap.put(boltId, load);
	}

	/**
	 * key based load for each bolt
	 * b1 -> {k1: 12, k2: 34, k3: 54}
	 */
	private void handleBoltLoad(String boltId, String key, Long value) {
		Map<String, Long> valueMap = boltLoadMap.get(boltId);
		if (valueMap == null) {
			valueMap = new HashMap<>();
			boltLoadMap.put(boltId, valueMap);
		}
		Long count = valueMap.get(key);
		if (count == null) {
			count = 0L;
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
		for (Map.Entry<String, Long> entry : relativeLoadMap.entrySet()) {
			Long load = entry.getValue();
			totalLoad += load;
		}
		for (Map.Entry<String, Long> entry : relativeLoadMap.entrySet()) {
			String boltId = entry.getKey();
			Long load = entry.getValue();
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
		for (Map.Entry<String, Map<String, Long>> entry : boltLoadMap.entrySet()) {
			String boltId = entry.getKey();
			Map<String, Long> valueMap = entry.getValue();
			int totalCount = 0;
			for (Map.Entry<String, Long> valueEntry : valueMap.entrySet()) {
				Long count = valueEntry.getValue();
				totalCount += count;
			}
			sb.append("bolt: ").append(boltId).append(NL);
			for (Map.Entry<String, Long> valueEntry : valueMap.entrySet()) {
				String value = valueEntry.getKey();
				Long count = valueEntry.getValue();
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
		for (Map.Entry<String, Map<String, Long>> loadEntry : valueLoadMap.entrySet()) {
			String key = loadEntry.getKey();
			Map<String, Long> boltMap = loadEntry.getValue();
			int totalCount = 0;
			for (Map.Entry<String, Long> boltEntry : boltMap.entrySet()) {
				Long count = boltEntry.getValue();
				totalCount += count;
			}
			sb.append("# ").append(key).append("\t[AGG: ").append(boltMap.size()).append("]").append(NL);
			for (Map.Entry<String, Long> boltEntry : boltMap.entrySet()) {
				String boltId = boltEntry.getKey();
				Long count = boltEntry.getValue();
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
		long NK = getNumberOfDistinctKeys();
		long NT = getNumberOfConsumedKeys();
		double MC = NK > 0 ? (double) NT / NK : 0;
		return "NK: " + NK +
				", NT: " + NT + " >> " +
				"Memory Consumption ratio is " + String.format("%.2f", MC);
	}

	public long getNumberOfDistinctKeys() {
		return valueLoadMap.size();
	}

	public long getNumberOfConsumedKeys() {
		int numberOfKeys = 0;
		for (Map<String, Long> keys : boltLoadMap.values()) {
			numberOfKeys += keys.size();
		}
		return numberOfKeys;
	}
}
