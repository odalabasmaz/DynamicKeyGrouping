package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import com.orhundalabasmaz.storm.utils.Statistics;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class DistributionAggregator {
	private Long totalCount = 0L;
	private final Map<String, Long> map = new HashMap<>();

	public void aggregate(String workerId, Long count) {
		totalCount += count;
		Long currCount = map.get(workerId);
		if (currCount == null) {
			map.put(workerId, count);
		} else {
			map.put(workerId, currCount + count);
		}
	}

	public double stdDev() {
		double[] metrics;
		synchronized (map) {
			int size = map.size();
			metrics = new double[size];
			int i = 0;
			for (Long value : map.values()) {
				metrics[i++] = 100 * (value / (double) totalCount);
			}
		}
		return Statistics.standardDeviation(metrics);
	}
}