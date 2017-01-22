package com.orhundalabasmaz.storm.loadbalancer.bolts.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.AggregationDetail;
import com.orhundalabasmaz.storm.model.TotalKeyCountMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class TotalKeyCountBolt extends BaseRichBolt {
	private transient OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		AggregationDetail detail = (AggregationDetail) tuple.getValueByField("aggregationDetail");
		Long timestamp = (Long) tuple.getValueByField("timestamp");
		Long totalKeyCount = detail.getTotalKeyCount();
		TotalKeyCountMessage message = new TotalKeyCountMessage(timestamp);
		message.setTotalKeyCount(totalKeyCount);
		collector.emit(tuple, new Values(message.getKey(), message));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
