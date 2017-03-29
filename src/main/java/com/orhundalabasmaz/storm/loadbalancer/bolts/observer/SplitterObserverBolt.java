package com.orhundalabasmaz.storm.loadbalancer.bolts.observer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.Aggregator;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.KeyAggregator;
import com.orhundalabasmaz.storm.loadbalancer.bolts.WindowedBolt;
import com.orhundalabasmaz.storm.model.Message;
import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class SplitterObserverBolt extends WindowedBolt {
	private transient OutputCollector collector;
	private transient Aggregator aggregator;
	private long startTime;
	private long totalCount;

	public SplitterObserverBolt(long tickFrequencyInSeconds) {
		super(tickFrequencyInSeconds);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.aggregator = new KeyAggregator();
	}

	@Override
	public synchronized void countDataAndAck(Tuple tuple) {
		if (startTime == 0) {
			emitFirstTime();
		}
		String key = tuple.getString(0);
		aggregator.aggregate(key);
		++totalCount;
		collector.ack(tuple);
	}

	private void emitFirstTime() {
		startTime = DKGUtils.getCurrentTimestamp();
		Message message = new Message("EVENT_INFO", startTime);
		message.addTag("EVENT_TYPE", "EVENT_START");
		message.addField("EVENT_TIME_FORMATTED", DKGUtils.formattedTime(startTime));
		message.addField("DURATION", 0);
		message.addField("TOTAL_COUNT", 0);
		collector.emit(new Values(message.getKey(), message));
	}

	@Override
	public synchronized void emitCurrentWindowAndAdvance() {
		long timestamp = DKGUtils.getCurrentTimestamp();
		Map<String, Long> counts = aggregator.getCountsThenAdvanceWindow();
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			Message message = new Message(key, timestamp);
			message.addTag("key", key);
			message.addField("count", count);
			collector.emit(new Values(message.getKey(), message));
		}
		emitOngoingTimeConsumption(totalCount, timestamp);
	}

	private void emitOngoingTimeConsumption(long totalCount, long timestamp) {
		Message message = new Message("EVENT_INFO", timestamp);
		message.addTag("EVENT_TYPE", "EVENT_ONGOING");
		message.addField("EVENT_TIME_FORMATTED", DKGUtils.formattedTime(timestamp));
		message.addField("DURATION", timestamp - startTime);
		message.addField("TOTAL_COUNT", totalCount);
		double incomingRatio = (double) totalCount / ((timestamp - startTime) / 1000);
		message.addField("INCOMING_RATIO", incomingRatio);
		collector.emit(new Values(message.getKey(), message));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
