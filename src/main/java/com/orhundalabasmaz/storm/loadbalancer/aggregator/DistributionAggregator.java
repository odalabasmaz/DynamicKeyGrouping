package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import com.orhundalabasmaz.storm.utils.Statistics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class DistributionAggregator implements Serializable {
	private Long totalCount = 0L;
	private final int numberOfWorkers;
	private final Map<String, Long> map = new HashMap<>();

	public DistributionAggregator(int numberOfWorkers) {
		this.numberOfWorkers = numberOfWorkers;
	}

	public void aggregate(String workerId, Long count) {
		totalCount += count;
		map.merge(workerId, count, (a, b) -> a + b);
	}

	public double stdDev() {
		double[] metrics;
		metrics = new double[numberOfWorkers];
		int i = 0;
		for (Long value : map.values()) {
			metrics[i++] = 100 * (value / (double) totalCount);
		}
		return Statistics.standardDeviation(metrics);
	}
}
