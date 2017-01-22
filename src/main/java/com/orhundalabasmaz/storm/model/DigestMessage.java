package com.orhundalabasmaz.storm.model;

/**
 * @author Orhun Dalabasmaz
 */
public class DigestMessage extends Message {
	private String value;
	private Long frequency;

	public DigestMessage(String key, long timestamp) {
		super(key, timestamp);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Long getFrequency() {
		return frequency;
	}

	public void setFrequency(Long frequency) {
		this.frequency = frequency;
	}
}
