package com.orhundalabasmaz.storm.common;

import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class Record {
	private long timestamp;
	private List<KeyCount> keyCounts;

	public Record() {
		this(DKGUtils.getCurrentTimestamp());
	}

	public Record(long timestamp) {
		this.timestamp = timestamp;
		this.keyCounts = new ArrayList<>();
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public List<KeyCount> getKeyCounts() {
		return keyCounts;
	}

	public void addKey(String key) {
		this.addKey(key, 1);
	}

	public void addKey(String key, long count) {
		this.keyCounts.add(new KeyCount(key, count));
	}
}
