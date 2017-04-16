package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public interface Aggregator {
	void aggregate(String key);

	void aggregate(String key, long count);

	Map<String, Long> getCountsThenAdvanceWindow();
}
