package com.orhundalabasmaz.storm.data.message;

import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterElectionMessage extends Message {
	private List<String> hashtags;
	private long timestamp;

	public TwitterElectionMessage(List<String> hashtags, long timestamp) {
		this.hashtags = hashtags;
		this.timestamp = timestamp;
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
