package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaClickstreamSplitter extends SplitterBolt {

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = JsonReader.readMessage(message);
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
