package com.orhundalabasmaz.storm.data.message;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewsMessage extends Message {
	private String page;
	private long timestamp;

	public WikipediaPageviewsMessage(String page, long timestamp) {
		this.page = page;
		this.timestamp = timestamp;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
