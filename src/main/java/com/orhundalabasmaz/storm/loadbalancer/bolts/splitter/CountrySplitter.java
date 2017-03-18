package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.Record;

/**
 * @author Orhun Dalabasmaz
 */
public class CountrySplitter extends JsonSplitter {

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = readMessage(message);
		String key = getCountry(jsonNode);
		Record record = new Record();
		record.addKey(key);
		return record;
	}

	private String getCountry(JsonNode jsonNode) {
		JsonNode country = jsonNode.get("country");
		return country.asText();
	}
}
