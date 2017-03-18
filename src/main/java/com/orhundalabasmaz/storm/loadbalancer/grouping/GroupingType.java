package com.orhundalabasmaz.storm.loadbalancer.grouping;

/**
 * @author Orhun Dalabasmaz
 */
public enum GroupingType {
	SHUFFLE("sg"),
	KEY("kg"),
	PARTIAL_KEY("pkg"),
	DYNAMIC_KEY("dkg");

	private String type;

	GroupingType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
