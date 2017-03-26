package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.WikipediaClickstreamMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaClickstreamProducer extends BaseProducer {

	public WikipediaClickstreamProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Long> map, String line, String fileName) {
		String[] parts = line.trim().split("\t");
		String prev = parts[0];
		String curr = parts[1];
		String type = parts[2];
		long n = Integer.parseInt(parts[3]);
		WikipediaClickstreamMessage message = new WikipediaClickstreamMessage(prev, curr, type, n);
		sendMessage(message);

		long count = map.getOrDefault(curr, 0L);
		map.put(curr, count + n);
		map.merge("TOTAL_COUNT", n, (a, b) -> a + b);
	}
}
