package com.orhundalabasmaz.storm.common;

/**
 * @author Orhun Dalabasmaz
 */
public interface ObjectObserver {
	default String getObjectId() {
		return this.toString().split("@")[1].toUpperCase();
	}
}
