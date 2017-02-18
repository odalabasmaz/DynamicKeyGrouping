package com.orhundalabasmaz.storm.loadbalancer.bolts.splitter;

import com.orhundalabasmaz.storm.common.Record;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;

/**
 * @author Orhun Dalabasmaz
 */
public class CountrySplitterBolt extends SplitterBolt {

	@Override
	protected Record convertMessage(String message) {
		return new Record(message);
	}
}
