package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.Aggregator;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.KeyAggregator;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerBolt extends WindowedBolt {
	private final Logger LOGGER = LoggerFactory.getLogger(WorkerBolt.class);
	private transient OutputCollector collector;
	private transient Aggregator aggregator;
	private long processDuration;
	private long aggregationDuration;

	public WorkerBolt(long tickFrequencyInSeconds, long processDuration, long aggregationDuration) {
		super(tickFrequencyInSeconds);
		this.processDuration = processDuration;
		this.aggregationDuration = aggregationDuration;
		LOGGER.info("WorkerBolt created with tickFrequencyInSeconds:{}, processDuration:{}, aggregationDuration:{}",
				tickFrequencyInSeconds, processDuration, aggregationDuration);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.aggregator = new KeyAggregator(aggregationDuration);
	}

	@Override
	public void countDataAndAck(Tuple tuple) {
		String key = (String) tuple.getValueByField("key");
		Long count = (Long) tuple.getValueByField("count");
		doToughJob();
		aggregator.aggregate(key, count);
		collector.ack(tuple);
	}

	@Override
	public void emitCurrentWindowAndAdvance() {
		Map<String, Long> counts = aggregator.getCountsThenAdvanceWindow();
		String workerId = getWorkerId();
		long timestamp = DKGUtils.getCurrentTimestamp();
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			collector.emit(new Values(workerId, key, count, timestamp));
		}
	}

	private void doToughJob() {
		DKGUtils.sleepInMilliseconds(processDuration);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("workerId", "key", "count", "timestamp"));
	}
}
