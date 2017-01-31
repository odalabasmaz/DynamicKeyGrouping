package com.orhundalabasmaz.storm.loadbalancer.aggregator;

import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class CountryAggregator implements Aggregator {
	private long aggregationDuration;

	// basic
	private final SortedMap<String, Long> counter;

	// detailed
	private long totalKeyCount;
	private final SortedMap<String, Long> workerCounts;
	private final SortedMap<String, CountInfo> keyCounts;

	public CountryAggregator() {
		this.counter = new TreeMap<>();
		this.totalKeyCount = 0L;
		this.workerCounts = new TreeMap<>();
		this.keyCounts = new TreeMap<>();
	}

	public CountryAggregator(long aggregationDuration) {
		this();
		this.aggregationDuration = aggregationDuration;
	}

	@Override
	public void aggregate(String key) {
		aggregate(key, 1);
	}

	@Override
	public void aggregate(String key, long count) {
		long curr = counter.containsKey(key) ? counter.get(key) : 0;
		counter.put(key, curr + count);
	}

	@Override
	public void aggregate(String workerId, Map<String, Long> counts) {
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			count(workerId, key, count);
		}
	}

	@Override
	public SortedMap<String, Long> getCountsThenAdvanceWindow() {
		final SortedMap<String, Long> dataClone = new TreeMap<>();
		final List<String> deleted = new ArrayList<>();
		synchronized (counter) {
			for (Map.Entry<String, Long> entry : counter.entrySet()) {
				dataClone.put(entry.getKey(), entry.getValue());
				deleted.add(entry.getKey());
			}
			for (String d : deleted) {
				counter.remove(d);
			}
		}
		return dataClone;
	}

	@Override
	public AggregationDetail getDetailedCountsThenAdvanceWindow() {
		AggregationDetail detail = new AggregationDetail();
		// total key count
		detail.setTotalKeyCount(totalKeyCount);

		// worker counts
		for (Map.Entry<String, Long> entry : workerCounts.entrySet()) {
			detail.addToWorkerCounts(entry.getKey(), entry.getValue());
		}

		// key counts
		for (Map.Entry<String, CountInfo> entry : keyCounts.entrySet()) {
			detail.addToKeyCounts(entry.getKey(), entry.getValue());
		}

		// reset detailed counts
		resetDetailedCounts();
		return detail;
	}

	private void resetDetailedCounts() {
		this.totalKeyCount = 0L;
		this.workerCounts.clear();
		this.keyCounts.clear();
	}

	private void count(String workerId, String key, Long count) {
		totalKeyCount += count;

		// update worker counts
		Long workerCount = workerCounts.get(workerId);
		if (workerCount == null) {
			workerCount = 0L;
		}
		workerCounts.put(workerId, workerCount + count);

		// update key counts
		CountInfo keyCount = keyCounts.get(key);
		if (keyCount == null) {
			keyCount = new CountInfo(key);
			keyCounts.put(key, keyCount);
		}
		keyCount.add(workerId, count);
	}

	public SortedMap<String, Long> getCounts() {
		return counter;
	}

	private void doToughJob(long duration) {
		DKGUtils.sleepInMilliseconds(duration);
	}
}
