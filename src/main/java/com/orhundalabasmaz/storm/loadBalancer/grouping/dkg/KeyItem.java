package com.orhundalabasmaz.storm.loadBalancer.grouping.dkg;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class KeyItem implements Comparable<KeyItem> {
	private String key;
	private long lastSeen;
	private long count;

	public KeyItem(String key) {
		this.key = key;
		this.count = 0;
		this.lastSeen = System.currentTimeMillis();
	}

	public String getKey() {
		return key;
	}

	public long getLastSeen() {
		return lastSeen;
	}

	public long getCount() {
		return count;
	}

	public void appearedAgain() {
		++count;
		lastSeen = System.currentTimeMillis();
	}

	@Override
	public int compareTo(KeyItem item) {
		return (int) (count - item.count);
	}

	@Override
	public int hashCode() {
		if (StringUtils.isBlank(key)) return 0;
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || !(obj instanceof KeyItem)) return false;
		KeyItem item = (KeyItem) obj;
		return this.key.equals(item.key);
	}
}