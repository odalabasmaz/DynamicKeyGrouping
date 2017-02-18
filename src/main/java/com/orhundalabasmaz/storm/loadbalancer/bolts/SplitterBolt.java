package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class SplitterBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(SplitterBolt.class);
	private transient OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		LOGGER.info("SplitterBolt# prepare: collector assigned");
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		String key = tuple.getString(0);
//		LOGGER.info("SplitterBolt# emitting new value: {}", key);
		collector.emit(tuple, new Values(key));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		LOGGER.info("bolt# output field declared: " + "splitter");
		outputFieldsDeclarer.declare(new Fields("key"));
	}
}
