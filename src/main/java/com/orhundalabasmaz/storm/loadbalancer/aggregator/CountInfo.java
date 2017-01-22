package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class CountInfo {
	private String key;
	private Long count;
	private Map<String, Long> workerCounts;

	public CountInfo(String key) {
		this.key = key;
		this.count = 0L;
		this.workerCounts = new LinkedHashMap<>();
	}

	public void add(String workerId, Long count) {
		this.count += count;
		Long workerCount = workerCounts.get(workerId);
		if (workerCount == null) {
			workerCount = 0L;
		}
		workerCounts.put(workerId, workerCount + count);
	}

	public String getKey() {
		return key;
	}

	public Long getCount() {
		return count;
	}

	public Map<String, Long> getWorkerCounts() {
		return workerCounts;
	}
}