package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterElectionSplitter extends SplitterBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterElectionSplitter.class);
	private static final Map<String, String> wordMap = new HashMap<>();
	private static final Map<String, String> charMap = new HashMap<>();

	static {
//		wordMap.put("", "");

		charMap.put("\n", " ");
		charMap.put("\u00e0", "a");
		charMap.put("\u00e1", "a");
		charMap.put("\u00e2", "a");
		charMap.put("\u00e3", "a");
		charMap.put("\u00e4", "a");
		charMap.put("\u00e5", "a");
		charMap.put("\u00e6", "a");
		charMap.put("\u00e7", "c");
		charMap.put("\u00e8", "e");
		charMap.put("\u00e9", "e");
	}

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
		return JsonReader.convertValue(jsHashtags, List.class);

		/*for (String hashtag : hashtags) {
			String key = replacement(hashtag);
			keys.add(key);
		}*/

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
	}

	private String replacement(String value) {
		String res = value.toLowerCase();

		// convert non-latin characters into latin characters (i.e: ê > e)
		for (Map.Entry<String, String> entry : charMap.entrySet()) {
			res = res.replaceAll(entry.getKey(), entry.getValue());
		}

		// replace synonym words
		if (wordMap.containsKey(res)) {
			res = wordMap.get(res);
		}

		return res;
	}

}
