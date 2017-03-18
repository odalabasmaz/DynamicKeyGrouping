package com.orhundalabasmaz.storm.common;

/**
 * @author Orhun Dalabasmaz
 */
public class KeyCount {
	private String key;
	private long count;

	public KeyCount(String key, long count) {
		this.key = key;
		this.count = count;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
}
