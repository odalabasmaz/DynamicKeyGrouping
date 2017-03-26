package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewsSplitter extends SplitterBolt {

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = JsonReader.readMessage(message);
		long timestamp = JsonReader.getTimestampInSeconds(jsonNode);
		String key = getPage(jsonNode);
		Record record = new Record(timestamp);
		record.addKey(key);
		return record;
	}

	private String getPage(JsonNode jsonNode) {
		String page = jsonNode.get("page").asText();
		return page.replaceFirst("http://en.wikipedia.org/wiki/", "");
	}
}
