package com.orhundalabasmaz.storm.loadBalancer.counter;

import com.orhundalabasmaz.storm.loadBalancer.Configuration;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;

import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class CountryCounter {
	private SortedMap<String, Integer> counter;

	public CountryCounter() {
		this.counter = new TreeMap<>(/*new Comparator<String>() {
	        public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        }*/);
	}

	public void count(String key) {
		int count = 1;
		if (counter.containsKey(key)) {
			count = counter.get(key) + 1;
		}
		counter.put(key, count);
		doToughJob(Configuration.PROCESS_DURATION);
	}

	public SortedMap<String, Integer> getCountsThenAdvanceWindow() {
		final SortedMap<String, Integer> dataClone = new TreeMap<>();
		final List<String> deleted = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : counter.entrySet()) {
			dataClone.put(entry.getKey(), entry.getValue());
			deleted.add(entry.getKey());
		}
		for (String d : deleted) {
			counter.remove(d);
		}
		return dataClone;
	}

	public void count(String key, Integer count) {
		if (counter.containsKey(key)) {
			doToughJob(Configuration.AGGREGATION_DURATION * (count - 1));
			count = counter.get(key) + count;
		}
		counter.put(key, count);
	}

	public SortedMap<String, Integer> getCounts() {
		return counter;
	}

	private void doToughJob(long duration) {
		DKGUtils.sleepInMilliseconds(duration);
	}
}
