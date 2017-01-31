package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import java.util.Map;
import java.util.SortedMap;

/**
 * @author Orhun Dalabasmaz
 */
public interface Aggregator {
	void aggregate(String key);

	void aggregate(String key, long count);

	void aggregate(String workerId, Map<String, Long> counts);

	SortedMap<String, Long> getCountsThenAdvanceWindow();

	AggregationDetail getDetailedCountsThenAdvanceWindow();
}
