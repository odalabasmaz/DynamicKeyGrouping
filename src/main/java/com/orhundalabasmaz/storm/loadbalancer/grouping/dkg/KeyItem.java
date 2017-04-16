package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Orhun Dalabasmaz
 */
public class KeyItem implements Comparable<KeyItem> {
	private String key;
	private long lastSeen;
	private long lastCheckTimeForScaling;
	private long count;
	private Set<Integer> targetTasks;

	public KeyItem(String key, int targetTask) {
		this.key = key;
		this.count = 1;
		this.lastSeen = System.currentTimeMillis();
		this.targetTasks = new LinkedHashSet<>();
		this.targetTasks.add(targetTask);
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

	public int getTargetTasksCount() {
		return targetTasks.size();
	}

	public long getLastCheckTimeForScaling() {
		return lastCheckTimeForScaling;
	}

	public void setLastCheckTimeForScaling(long time) {
		this.lastCheckTimeForScaling = time;
	}

	public void appearedAgain(int targetTask) {
		++count;
		targetTasks.add(targetTask);
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