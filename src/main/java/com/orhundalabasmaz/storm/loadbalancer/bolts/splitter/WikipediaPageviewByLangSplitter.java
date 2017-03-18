package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.Record;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewByLangSplitter extends JsonSplitter {

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = readMessage(message);
		String key = getLang(jsonNode);
		long count = getCount(jsonNode);
		Record record = new Record();
		record.addKey(key, count);
		return record;
	}

	private String getLang(JsonNode jsonNode) {
		return jsonNode.get("lang").asText();
	}

	private long getCount(JsonNode jsonNode) {
		return jsonNode.get("n").asLong();
	}

}
