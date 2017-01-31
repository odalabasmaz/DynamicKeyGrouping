package com.orhundalabasmaz.storm.model;

import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class Message {
	private String key;
	private String messageId;
	private long timestamp;
	private Map<String, String> tags;
	private Map<String, Object> fields;

	public Message() {
		this(null);
	}

	public Message(String key) {
		this(key, DKGUtils.generateUUID(), DKGUtils.getCurrentTimestamp());
	}

	public Message(String key, long timestamp) {
		this(key, DKGUtils.generateUUID(), timestamp);
	}

	public Message(String key, String messageId) {
		this(key, messageId, DKGUtils.getCurrentTimestamp());
	}

	public Message(String key, String messageId, long timestamp) {
		this.key = key;
		this.messageId = messageId;
		this.timestamp = timestamp;
		this.tags = new LinkedHashMap<>();
		this.fields = new LinkedHashMap<>();
	}

	public String getKey() {
		return key;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void addTag(String key, String value) {
		this.tags.put(key, value);
	}

	public Map<String, Object> getFields() {
		return fields;
	}

	public void addField(String key, Object value) {
		this.fields.put(key, value);
	}
}
