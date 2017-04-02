package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.common.KeyCount;
import com.orhundalabasmaz.storm.common.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class SplitterBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(SplitterBolt.class);
	private transient OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		LOGGER.info("SplitterBolt# prepare: collector assigned");
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		collector.ack(tuple);
		synchronized (this) {
			String message = tuple.getString(0);
			Record record = convertMessage(message);
			Long timestamp = record.getTimestamp();
			List<KeyCount> keyCounts = record.getKeyCounts();
			for (KeyCount keyCount : keyCounts) {
				collector.emit(tuple, new Values(keyCount.getKey(), keyCount.getCount(), timestamp));
			}
		}
//		collector.ack(tuple);
	}

	protected abstract Record convertMessage(String message);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		LOGGER.info("bolt# output field declared: " + "splitter");
		outputFieldsDeclarer.declare(new Fields("key", "count", "timestamp"));
	}

}
