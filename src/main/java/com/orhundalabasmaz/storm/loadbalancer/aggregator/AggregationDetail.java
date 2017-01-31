package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Orhun Dalabasmaz
 */
public class AggregationDetail {
	private Long totalKeyCount;
	private SortedMap<String, Long> workerCounts;
	private SortedMap<String, CountInfo> keyCounts;

	public AggregationDetail() {
		workerCounts = new TreeMap<>();
		keyCounts = new TreeMap<>();
	}

	public void setTotalKeyCount(Long totalKeyCount) {
		this.totalKeyCount = totalKeyCount;
	}

	public void addToWorkerCounts(String workerId, Long count) {
		Long workerCount = workerCounts.get(workerId);
		if (workerCount == null) {
			workerCount = 0L;
		}
		workerCounts.put(workerId, workerCount + count);
	}

	public void addToKeyCounts(String key, CountInfo countInfo) {
		CountInfo keyCount = keyCounts.get(key);
		if (keyCount == null) {
			keyCount = new CountInfo(key);
			keyCounts.put(key, keyCount);
		}
		for (Map.Entry<String, Long> entry : countInfo.getWorkerCounts().entrySet()) {
			String workerId = entry.getKey();
			Long count = entry.getValue();
			keyCount.add(workerId, count);
		}
	}

	public Long getTotalKeyCount() {
		return totalKeyCount;
	}

	public SortedMap<String, Long> getWorkerCounts() {
		return workerCounts;
	}

	public SortedMap<String, CountInfo> getKeyCounts() {
		return keyCounts;
	}
}
