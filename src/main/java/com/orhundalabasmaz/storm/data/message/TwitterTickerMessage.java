package com.orhundalabasmaz.storm.data.message;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterTickerMessage extends Message {
	private String ticker;
	private long timestamp;

	public TwitterTickerMessage(String ticker, long timestamp) {
		this.ticker = ticker;
		this.timestamp = timestamp;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
