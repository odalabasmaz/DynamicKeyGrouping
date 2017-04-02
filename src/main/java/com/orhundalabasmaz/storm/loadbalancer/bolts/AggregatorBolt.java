package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.Aggregator;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.KeyAggregator;
import com.orhundalabasmaz.storm.model.Message;
import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class AggregatorBolt extends WindowedBolt {
	private transient OutputCollector collector;
	private transient Aggregator aggregator;
	private long aggregationDuration;

	public AggregatorBolt(long tickFrequencyInSeconds, long aggregationDuration) {
		super(tickFrequencyInSeconds);
		this.aggregationDuration = aggregationDuration;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.aggregator = new KeyAggregator(aggregationDuration);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}

	@Override
	@SuppressWarnings("unchecked")
	public void countDataAndAck(Tuple tuple) {
		String key = (String) tuple.getValueByField("key");
		Long count = (Long) tuple.getValueByField("count");
		aggregator.aggregate(key, count);
		collector.ack(tuple);
	}

	@Override
	public void emitCurrentWindowAndAdvance() {
		Map<String, Long> counts = aggregator.getCountsThenAdvanceWindow();
		long timestamp = DKGUtils.getCurrentTimestamp();
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			Message message = new Message(key, timestamp);
			message.addTag("key", key);
			message.addField("count", count);
			collector.emit(new Values(key, message));
		}
	}
}
