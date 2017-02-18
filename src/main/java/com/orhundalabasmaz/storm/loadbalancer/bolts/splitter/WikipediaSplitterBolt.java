package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaSplitterBolt extends SplitterBolt {

	@Override
	protected Record convertMessage(String message) {
		String[] part = message.split(" ");
		long timestamp = Long.parseLong(part[0]);
		String key = part[1].replaceFirst("http://en.wikipedia.org/wiki/", "");
		return new Record(timestamp, key);
	}
}
