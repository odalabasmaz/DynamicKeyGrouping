package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.TwitterTickerMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterTickerProducer extends BaseProducer {

	public TwitterTickerProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Integer> map, String line, String fileName) {
		String[] parts = line.trim().split("\t");
		long timestamp = Long.parseLong(parts[0]);
		String ticker = parts[1];
		TwitterTickerMessage message = new TwitterTickerMessage(ticker, timestamp);
		sendMessage(message);

		int keyCount = map.getOrDefault(ticker, 0);
		map.put(ticker, keyCount + 1);
	}
}
