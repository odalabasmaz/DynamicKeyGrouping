package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterElectionSplitter extends SplitterBolt {
	private static final List<String> ELECTION_DAY_KEYS = Arrays.asList(
			"hilary", "hillary", "clinton", "withher", "wither", "voteher", "lockherup",
			"donald", "trump", "makeamericagreatagain", "americafirst",
			"vote", "voting", "poll",
			"election", "eleicoes", "eleccion", "electon", "electoral", "abdseçimleri",
			"president", "debate", "democrat", "republican",
			"america", "us", "usa"
	);

	@Override
	protected Record convertMessage(String message) {
		JsonNode jsonNode = JsonReader.readMessage(message);
		long timestamp = JsonReader.getTimestamp(jsonNode);
		List<String> keys = getKeys(jsonNode);
		Record record = new Record(timestamp);
		for (String key : keys) {
			record.addKey(key);
		}
		return record;
	}

	@SuppressWarnings("unchecked")
	private List<String> getKeys(JsonNode jsonNode) {
		// hashtags
		JsonNode jsHashtags = jsonNode.get("hashtags");
		List<String> hashtags = JsonReader.convertValue(jsHashtags, List.class);
		List<String> keys = new ArrayList<>(hashtags.size());

		// hashtags
		for (String hashtag : hashtags) {
			if (hashtag.contains("?")) {
				continue;
			}
			String key = replacement(hashtag);
			keys.add(key);
		}

		// text
		/*JsonNode jsText = jsonNode.get("text");
		String[] words = jsText
				.asText()
				.replaceAll("[^\\p{L}\\p{Nd}]+", " ")
				.split(" ");
		for (String word : words) {
			String key = replacement(word);
			keys.add(key);
		}*/

		return keys;
	}

	private String replacement(String value) {
		String val = value.toLowerCase();
		for (String key : ELECTION_DAY_KEYS) {
			if (val.contains(key)) {
				return "ElectionDay";
			}
		}
		return value;
	}
}
