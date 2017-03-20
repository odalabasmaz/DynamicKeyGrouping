package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.WikipediaPageviewMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewProducer extends BaseProducer {

	public WikipediaPageviewProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Integer> map, String line, String fileName) {
		String[] parts = line.trim().split("\t");
		long timestamp = Long.parseLong(parts[0]);
		String page = parts[1];
		WikipediaPageviewMessage message = new WikipediaPageviewMessage(page, timestamp);
		sendMessage(message);

		//java.lang.OutOfMemoryError: Java heap space
//		int count = map.getOrDefault(page, 0);
//		map.put(page, count + 1);
	}
}
