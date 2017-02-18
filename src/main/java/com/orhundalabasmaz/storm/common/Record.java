package com.orhundalabasmaz.storm.common;

import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class Record {
	private long timestamp;
	private List<String> keys;

	public Record() {
		this(DKGUtils.getCurrentTimestamp());
	}

	public Record(long timestamp) {
		this.timestamp = timestamp;
		this.keys = new ArrayList<>();
	}

	public Record(String key) {
		this(DKGUtils.getCurrentTimestamp(), key);
	}

	public Record(long timestamp, String key) {
		this.timestamp = timestamp;
		this.keys = Arrays.asList(key);
	}

	public Record(long timestamp, List<String> keys) {
		this.timestamp = timestamp;
		this.keys = keys;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public List<String> getKeys() {
		return keys;
	}

	public void addKey(String key) {
		this.keys.add(key);
	}
}
