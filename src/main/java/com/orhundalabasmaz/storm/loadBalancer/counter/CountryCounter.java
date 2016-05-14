package com.orhundalabasmaz.storm.loadBalancer.counter;

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

	public void count(String country) {
		int count = 1;
		if (counter.containsKey(country)) {
			count = counter.get(country) + 1;
		}
		counter.put(country, count);
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

	public void count(String country, Integer count) {
		if (counter.containsKey(country)) {
			count = counter.get(country) + count;
		}
		counter.put(country, count);
	}

	public SortedMap<String, Integer> getCounts() {
		return counter;
	}
}
