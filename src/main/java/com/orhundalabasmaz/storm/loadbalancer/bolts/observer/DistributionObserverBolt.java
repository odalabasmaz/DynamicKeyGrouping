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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Orhun Dalabasmaz
 */
public class DistributionObserverBolt extends WindowedBolt {
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
	protected void countDataAndAck(Tuple tuple) {
		synchronized (this) {
			String workerId = (String) tuple.getValueByField("workerId");
			String key = (String) tuple.getValueByField("key");
			Long count = (Long) tuple.getValueByField("count");
//			Long timestamp = (Long) tuple.getValueByField("timestamp");
			distributionAggregator.aggregate(workerId, count);

			// aggregate total count
			totalCount += count;

			keyWorkers.putIfAbsent(key, new HashSet<>());
			Set<String> workerSet = keyWorkers.get(key);
			workerSet.add(workerId);
			collector.ack(tuple);
		}
	}

	@Override
	protected void emitCurrentWindowAndAdvance() {
		long timestamp = DKGUtils.getCurrentTimestamp();
		synchronized (this) {
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
//			keyWorkers.clear();

			// emit stddev
			emitStandardDeviation(timestamp);

			// calculated distribution cost
			emitCalculatedDistributionCost(timestamp, totalKeys, distinctKeys);

			// total emitted count & total time consumption
			emitTotalCountsAndTimeConsumption(timestamp);
		}
	}

	private void emitStandardDeviation(long timestamp) {
		double stdDev = distributionAggregator.stdDev();
		Message message = new Message("EVENT_INFO", timestamp);
		message.addTag("EVENT_TYPE", "STD_DEV");
		message.addField("EVENT_TIME", timestamp);
		message.addField("EVENT_TIME_FORMATTED", DKGUtils.formattedTime(timestamp));
		message.addField("STD_DEV", stdDev);
		collector.emit(new Values(message.getKey(), message));
	}

	private void emitCalculatedDistributionCost(long timestamp, int totalKeys, double distinctKeys) {
		String key = "CALC_DIST_COST";
		double distCost = distinctKeys > 0 ? totalKeys / distinctKeys : 0;
		Message message = new Message(key, timestamp);
		message.addField(key, distCost);
		collector.emit(new Values(message.getKey(), message));
	}

	private void emitTotalCountsAndTimeConsumption(long timestamp) {
		Message message = new Message("EVENT_INFO", timestamp);
		message.addTag("EVENT_TYPE", "EVENT_ONGOING");
		message.addField("EVENT_TIME", timestamp);
		message.addField("EVENT_TIME_FORMATTED", DKGUtils.formattedTime(timestamp));
		message.addField("TOTAL_TIME_CONSUMPTION", timestamp - startTime);
		if (totalCount > latestTotalCount) {
			latestTotalCount = totalCount;
			latestTimeConsumption = timestamp - startTime;
		}
		message.addField("LATEST_TIME_CONSUMPTION", latestTimeConsumption);
		message.addField("TOTAL_COUNT", totalCount);
		double throughputRatio = (double) totalCount / ((timestamp - startTime) / 1000);
		message.addField("THROUGHPUT_RATIO", throughputRatio);
		collector.emit(new Values(message.getKey(), message));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
