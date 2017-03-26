package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.WikipediaPageviewsByLangMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewsByLangProducer extends BaseProducer {
	private static Logger LOGGER = LoggerFactory.getLogger(WikipediaPageviewsByLangProducer.class);

	public WikipediaPageviewsByLangProducer(String filePath, String topicName) {
		super(filePath, topicName);
	}

	@Override
	public void produce(Map<String, Long> map, String line, String fileName) {
		String[] parts = line.trim().split(" ");
		if (parts.length != 4) {
			LOGGER.error("Unexpected line: \"{}\" in {}", line, fileName);
			return;
		}
		String lang = parts[0];
		String page = parts[1];
		long n = Integer.parseInt(parts[2]);
		long m = Integer.parseInt(parts[3]);
		WikipediaPageviewsByLangMessage message = new WikipediaPageviewsByLangMessage(lang, page, n, m);
		sendMessage(message);

		long count = map.getOrDefault(lang, 0L);
		map.put(lang, count + n);
		map.merge("TOTAL_COUNT", n, (a, b) -> a + b);
	}
}
