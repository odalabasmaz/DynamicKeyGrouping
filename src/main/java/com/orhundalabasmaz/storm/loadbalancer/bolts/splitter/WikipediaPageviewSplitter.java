package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.Record;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewSplitter extends JsonSplitter {

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = readMessage(message);
		long timestamp = getTimestampInSeconds(jsonNode);
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
