package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class JsonSplitter extends SplitterBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonSplitter.class);
	protected static final ObjectMapper objectMapper = new ObjectMapper();

	protected JsonNode readMessage(String message) {
		JsonNode jsonNode = null;
		try {
			jsonNode = objectMapper.readTree(message);
		} catch (IOException e) {
			LOGGER.error("Record cannot be constructed.", e);
		}
		return jsonNode;
	}

	protected long getTimestamp(JsonNode jsonNode) {
		return jsonNode.get("timestamp").asLong();
	}

	protected long getTimestampInSeconds(JsonNode jsonNode) {
		return getTimestamp(jsonNode) * 1000;
	}
}
