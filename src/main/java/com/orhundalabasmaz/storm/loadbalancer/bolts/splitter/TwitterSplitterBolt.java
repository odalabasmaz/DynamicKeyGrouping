package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterSplitterBolt extends SplitterBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSplitterBolt.class);

	@Override
	@SuppressWarnings("unchecked")
	protected Record convertMessage(String message) {
		Record record = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			JsonNode jsonNode = objectMapper.readTree(message);
			long timestamp = jsonNode.get("timestamp").asLong();
			List<String> keys = objectMapper.convertValue(jsonNode.get("hashtags"), List.class);
			record = new Record(timestamp, keys);
		} catch (IOException e) {
			LOGGER.error("Record cannot be constructed.", e);
		}
		return record;
	}
}
