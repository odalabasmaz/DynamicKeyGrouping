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

	public static String[] keys() {
		return new String[]{SHUFFLE.getType(), KEY.getType(), PARTIAL_KEY.getType(), DYNAMIC_KEY.getType()};
	}
}
