package com.orhundalabasmaz.storm.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Orhun Dalabasmaz
 */
public class JsonReader {
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonReader.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	private JsonReader() {
	}

	public static JsonNode readMessage(String message) {
		JsonNode jsonNode = null;
		try {
			jsonNode = objectMapper.readTree(message);
		} catch (IOException e) {
			LOGGER.error("Record cannot be constructed.", e);
		}
		return jsonNode;
	}

	public static long getTimestamp(JsonNode jsonNode) {
		JsonNode timestamp = jsonNode.get("timestamp");
		if (timestamp == null) {
			timestamp = jsonNode.get("timestamp_ms");
		}
		return timestamp.asLong();
	}

	public static long getTimestampInSeconds(JsonNode jsonNode) {
		return getTimestamp(jsonNode) * 1000;
	}

	public static <T> T convertValue(Object fromValue, Class<T> clazz) {
		return objectMapper.convertValue(fromValue, clazz);
	}
}
