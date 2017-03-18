package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaClickstreamSplitter extends JsonSplitter {
	private static final Logger LOGGER = LoggerFactory.getLogger(WikipediaClickstreamSplitter.class);

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = readMessage(message);
		String key = getCurr(jsonNode);
		long count = getCount(jsonNode);
		Record record = new Record();
		record.addKey(key, count);
		return record;
	}

	private String getCurr(JsonNode jsonNode) {
		return jsonNode.get("curr").asText();
	}

	private long getCount(JsonNode jsonNode) {
		return jsonNode.get("n").asLong();
	}
}
