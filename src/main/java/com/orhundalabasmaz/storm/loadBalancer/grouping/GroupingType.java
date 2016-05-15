package com.orhundalabasmaz.storm.loadBalancer.grouping;

/**
 * @author Orhun Dalabasmaz
 */
public enum GroupingType {
	SHUFFLE,
	KEY,
	PARTIAL_KEY,
	DYNAMIC_KEY
}
