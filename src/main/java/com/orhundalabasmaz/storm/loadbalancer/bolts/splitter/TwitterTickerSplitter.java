package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterTickerSplitter extends SplitterBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterTickerSplitter.class);

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = JsonReader.readMessage(message);
		long timestamp = JsonReader.getTimestampInSeconds(jsonNode);
		String key = getTicker(jsonNode);
		Record record = new Record(timestamp);
		record.addKey(key);
		return record;
	}

	private String getTicker(JsonNode jsonNode) {
		JsonNode ticker = jsonNode.get("ticker");
		return ticker.asText();
	}
}
