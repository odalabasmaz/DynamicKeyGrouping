package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;

/**
 * @author Orhun Dalabasmaz
 */
public class CountrySplitter extends SplitterBolt {

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = JsonReader.readMessage(message);
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
