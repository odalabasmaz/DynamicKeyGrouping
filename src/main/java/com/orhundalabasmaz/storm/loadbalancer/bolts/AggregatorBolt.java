package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.Aggregator;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.KeyAggregator;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class AggregatorBolt extends WindowedBolt {
	private final Logger LOGGER = LoggerFactory.getLogger(AggregatorBolt.class);
	private transient OutputCollector collector;
	private transient Aggregator aggregator;
	private long lastAckTime;
	private long lastEmitTime;

	public AggregatorBolt(long tickFrequencyInSeconds) {
		super(tickFrequencyInSeconds);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.aggregator = new KeyAggregator();
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
		lastAckTime = DKGUtils.getCurrentTimestamp();
		collector.ack(tuple);
	}

	@Override
	public void emitCurrentWindowAndAdvance() {
		if (lastEmitTime >= lastAckTime) {
			StringBuilder sb = new StringBuilder();
			Map<String, Long> counts = aggregator.getCountsThenAdvanceWindow();
			for (Map.Entry<String, Long> entry : counts.entrySet()) {
				String key = entry.getKey();
				Long count = entry.getValue();
				sb.append(key).append(",").append(count).append("\n");
			}
			LOGGER.info("#AGG: Key Counts\n{}", sb);
		} else {
			lastEmitTime = DKGUtils.getCurrentTimestamp();
		}

		/*Map<String, Long> counts = aggregator.getCountsThenAdvanceWindow();
		long timestamp = DKGUtils.getCurrentTimestamp();
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			Message message = new Message(key, timestamp);
			message.addTag("key", key);
			message.addField("count", count);
			collector.emit(new Values(key, message));
		}*/
//		LOGGER.info("#AC: aggregator.counts.size() = {}", counts.size());
	}
}
