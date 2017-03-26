package com.orhundalabasmaz.storm.data.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.orhundalabasmaz.storm.common.JsonReader;
import com.orhundalabasmaz.storm.data.message.TwitterElectionMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterElectionProducer extends BaseProducer {

	public TwitterElectionProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void produce(Map<String, Long> map, String line, String fileName) {
		JsonNode jsonNode = JsonReader.readMessage(line);
		if (jsonNode.get("limit") != null) {
			return;
		}

		long timestamp = JsonReader.getTimestamp(jsonNode);
		String text = jsonNode.get("text").asText();

		JsonNode jsHashtags = jsonNode.get("entities").get("hashtags");
		List<Map<String, String>> hashtags = JsonReader.convertValue(jsHashtags, List.class);

		List<String> hashtagList = new ArrayList<>(hashtags.size());
		for (Map<String, String> ht : hashtags) {
			String hashtag = ht.get("text");
			hashtagList.add(hashtag);
			long keyCount = map.getOrDefault(hashtag, 0L);
			map.put(hashtag, keyCount + 1);
			map.merge("TOTAL_COUNT", 1L, (a, b) -> a + b);
		}

		TwitterElectionMessage message = new TwitterElectionMessage(text, hashtagList, timestamp);
		sendMessage(message);
	}
}
