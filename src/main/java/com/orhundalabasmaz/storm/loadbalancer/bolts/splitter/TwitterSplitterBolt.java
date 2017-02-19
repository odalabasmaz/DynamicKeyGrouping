package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterSplitterBolt extends SplitterBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSplitterBolt.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final Map<String, String> replacementMap = new HashMap<>();

	static {
//		replacementMap.put("", "");
	}

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = readMessage(message);
		long timestamp = getTimestamp(jsonNode);
		List<String> keys = getKeys(jsonNode);
		return new Record(timestamp, keys);
	}

	private JsonNode readMessage(String message) {
		JsonNode jsonNode = null;
		try {
			jsonNode = objectMapper.readTree(message);
		} catch (IOException e) {
			LOGGER.error("Record cannot be constructed.", e);
		}
		return jsonNode;
	}

	private long getTimestamp(JsonNode jsonNode) {
		return jsonNode.get("timestamp").asLong();
	}

	@SuppressWarnings("unchecked")
	private List<String> getKeys(JsonNode jsonNode) {
		List<String> hashtags = objectMapper.convertValue(jsonNode.get("hashtags"), List.class);
		List<String> keys = new ArrayList<>(hashtags.size());
		for (String hashtag : hashtags) {
			String tag = replacement(hashtag);
			keys.add(tag);
		}
		return keys;
	}

	private String replacement(String hashtag) {
		String tag = hashtag.toLowerCase();
		if (replacementMap.containsKey(tag)) {
			return replacementMap.get(tag);
		}
		return tag;
	}

}
