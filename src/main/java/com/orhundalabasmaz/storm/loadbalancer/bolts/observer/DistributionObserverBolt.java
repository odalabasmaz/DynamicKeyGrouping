package com.orhundalabasmaz.storm.loadbalancer.bolts.observer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.DistributionAggregator;
import com.orhundalabasmaz.storm.loadbalancer.bolts.WindowedBolt;
import com.orhundalabasmaz.storm.model.Message;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Orhun Dalabasmaz
 */
public class DistributionObserverBolt extends WindowedBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(DistributionObserverBolt.class);
	private transient OutputCollector collector;
	private Map<String, Set<String>> keyWorkers;
	private DistributionAggregator distributionAggregator;
	private long startTime;
	private long totalCount;
	private long latestTotalCount;
	private long latestTimeConsumption;

	public DistributionObserverBolt(long tickFrequencyInSeconds) {
		super(tickFrequencyInSeconds);
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.distributionAggregator = new DistributionAggregator();
		this.keyWorkers = new LinkedHashMap<>();
		this.startTime = DKGUtils.getCurrentTimestamp();
	}

	@Override
	protected synchronized void countDataAndAck(Tuple tuple) {
		String workerId = (String) tuple.getValueByField("workerId");
		String key = (String) tuple.getValueByField("key");
		Long count = (Long) tuple.getValueByField("count");
//			Long timestamp = (Long) tuple.getValueByField("timestamp");
		distributionAggregator.aggregate(workerId, count);

		// aggregate total count
		totalCount += count;
		updateTimeConsumption();

		keyWorkers.putIfAbsent(key, new HashSet<>());
		Set<String> workerSet = keyWorkers.get(key);
		workerSet.add(workerId);
		collector.ack(tuple);
	}

	@Override
	protected synchronized void emitCurrentWindowAndAdvance() {
		long timestamp = DKGUtils.getCurrentTimestamp();
		int totalKeys = 0;
		int distinctKeys = keyWorkers.keySet().size();
		for (Map.Entry<String, Set<String>> entry : keyWorkers.entrySet()) {
			String key = entry.getKey();
			int numberOfWorkers = entry.getValue().size();
			totalKeys += numberOfWorkers;
			Message message = new Message(key, timestamp);
			message.addTag("key", key);
			message.addField("numberOfWorkers", numberOfWorkers);
			collector.emit(new Values(message.getKey(), message));
		}

		if (totalCount > latestTotalCount) {
			latestTotalCount = totalCount;
			calculateDistribution(timestamp, totalKeys, distinctKeys);
		}
	}

	private void updateTimeConsumption() {
		long timetamp = DKGUtils.getCurrentTimestamp();
		latestTimeConsumption = timetamp - startTime;
	}

	private void calculateDistribution(long timestamp, int totalKeys, double distinctKeys) {
		double stdDev = distributionAggregator.stdDev();
		double distCost = distinctKeys > 0 ? totalKeys / distinctKeys : 0;
		double throughputRatio = (double) totalCount / (latestTimeConsumption / 1000);

		Message message = new Message("EVENT_INFO", timestamp);
		message.addField("TOTAL_COUNT", totalCount);
		message.addField("DURATION", latestTimeConsumption);
		message.addField("STD_DEV", stdDev);
		message.addField("DIST_COST", distCost);
		message.addField("THROUGHPUT_RATIO", throughputRatio);
		collector.emit(new Values(message.getKey(), message));
		LOGGER.info("### TotalCount:{}, Duration: {}ms, StdDev: {}, DistCost: {}, ThroughputRatio: {}",
				totalCount, latestTimeConsumption, stdDev, distCost, throughputRatio);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
