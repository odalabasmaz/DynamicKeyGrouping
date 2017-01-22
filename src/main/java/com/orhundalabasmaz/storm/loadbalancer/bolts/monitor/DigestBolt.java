package com.orhundalabasmaz.storm.loadbalancer.bolts.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.AggregationDetail;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.CountInfo;
import com.orhundalabasmaz.storm.model.DigestMessage;

import java.util.Map;
import java.util.Set;

/**
 * @author Orhun Dalabasmaz
 */
public class DigestBolt extends BaseRichBolt {
	private transient OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		AggregationDetail detail = (AggregationDetail) tuple.getValueByField("aggregationDetail");
		Long timestamp = (Long) tuple.getValueByField("timestamp");
		for (Map.Entry<String, CountInfo> entry : detail.getKeyCounts().entrySet()) {
			String key = entry.getKey();
			CountInfo countInfo = entry.getValue();
			Long frequency = countInfo.getCount();
			DigestMessage message = new DigestMessage(key, timestamp);
			message.setValue(key);
			message.setFrequency(frequency);
			collector.emit(tuple, new Values(message.getKey(), message));
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
