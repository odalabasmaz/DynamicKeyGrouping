package com.orhundalabasmaz.storm.loadbalancer.grouping;

/**
 * @author Orhun Dalabasmaz
 */
public enum GroupingType {
	SHUFFLE,
	KEY,
	PARTIAL_KEY,
	DYNAMIC_KEY
}
