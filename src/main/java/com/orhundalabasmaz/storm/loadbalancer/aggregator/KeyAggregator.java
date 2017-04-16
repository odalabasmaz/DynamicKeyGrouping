package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class KeyAggregator implements Aggregator {
	private Map<String, Long> counter;

	public KeyAggregator() {
		this.counter = new HashMap<>();
	}

	@Override
	public void aggregate(String key) {
		aggregate(key, 1);
	}

	@Override
	public void aggregate(String key, long count) {
		counter.merge(key, count, (a, b) -> a + b);
	}

	@Override
	public Map<String, Long> getCountsThenAdvanceWindow() {
		Map<String, Long> dataClone = new HashMap<>();
		for (Map.Entry<String, Long> entry : counter.entrySet()) {
			dataClone.put(entry.getKey(), entry.getValue());
		}
		counter.clear();
		return dataClone;
	}
}
