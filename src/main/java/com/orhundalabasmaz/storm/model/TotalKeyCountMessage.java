package com.orhundalabasmaz.storm.model;

/**
 * @author Orhun Dalabasmaz
 */
public class TotalKeyCountMessage extends Message {
	private static final String KEY = "totalKeyCount";
	private Long totalKeyCount;

	public TotalKeyCountMessage(long timestamp) {
		super(KEY, timestamp);
	}

	public Long getTotalKeyCount() {
		return totalKeyCount;
	}

	public void setTotalKeyCount(Long totalKeyCount) {
		this.totalKeyCount = totalKeyCount;
	}
}
