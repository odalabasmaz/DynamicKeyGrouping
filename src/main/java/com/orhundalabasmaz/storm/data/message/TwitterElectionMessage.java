package com.orhundalabasmaz.storm.data.message;

import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterElectionMessage extends Message {
	private String text;
	private List<String> hashtags;
	private long timestamp;

	public TwitterElectionMessage(String text, List<String> hashtags, long timestamp) {
		this.text = text;
		this.hashtags = hashtags;
		this.timestamp = timestamp;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public List<String> getHashtags() {
		return hashtags;
	}

	public void setHashtags(List<String> hashtags) {
		this.hashtags = hashtags;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
