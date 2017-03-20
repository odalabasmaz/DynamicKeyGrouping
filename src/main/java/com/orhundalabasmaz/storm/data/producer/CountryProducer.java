package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.CountryMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class CountryProducer extends BaseProducer {

	public CountryProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Integer> map, String line, String fileName) {
		String country = line.trim();
		CountryMessage message = new CountryMessage(country);
		sendMessage(message);

		int count = map.getOrDefault(country, 0);
		map.put(country, count + 1);
	}
}
