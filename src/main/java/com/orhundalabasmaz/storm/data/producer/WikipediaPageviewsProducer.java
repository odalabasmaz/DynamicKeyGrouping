package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.WikipediaPageviewsMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewsProducer extends BaseProducer {

	public WikipediaPageviewsProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Long> map, String line, String fileName) {
		String[] parts = line.trim().split("\t");
		long timestamp = Long.parseLong(parts[0]);
		String page = parts[1];
		WikipediaPageviewsMessage message = new WikipediaPageviewsMessage(page, timestamp);
		sendMessage(message);

		//java.lang.OutOfMemoryError: Java heap space
		long count = map.getOrDefault(page, 0L);
		map.put(page, count + 1);
		map.merge("TOTAL_COUNT", 1L, (a, b) -> a + b);
	}
}
