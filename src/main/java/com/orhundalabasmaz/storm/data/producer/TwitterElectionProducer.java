package com.orhundalabasmaz.storm.data.producer;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class TwitterElectionProducer extends BaseProducer {

	public TwitterElectionProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Integer> map, String line, String fileName) {
		sendMessage(line);
	}
}
