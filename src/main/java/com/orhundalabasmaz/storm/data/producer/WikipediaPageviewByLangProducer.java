package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.WikipediaPageviewByLangMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewByLangProducer extends BaseProducer {

	public WikipediaPageviewByLangProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Integer> map, String line) {
		String[] parts = line.trim().split("\t");
		String lang = parts[0];
		String page = parts[1];
		int n = Integer.parseInt(parts[2]);
		int m = Integer.parseInt(parts[3]);
		WikipediaPageviewByLangMessage message = new WikipediaPageviewByLangMessage(lang, page, n, m);
		sendMessage(message);

		int count = map.getOrDefault(lang, 0);
		map.put(lang, count + n);
	}
}
