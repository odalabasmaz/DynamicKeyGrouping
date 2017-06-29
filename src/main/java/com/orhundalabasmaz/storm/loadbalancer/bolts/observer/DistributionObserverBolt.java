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

import java.util.HashMap;
import java.util.LinkedHashSet;
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
	private long totalWorkers;
	private boolean canPrintOut;

	public DistributionObserverBolt(int numberOfWorkers, long tickFrequencyInSeconds) {
		super(tickFrequencyInSeconds);
		this.distributionAggregator = new DistributionAggregator(numberOfWorkers);
		this.keyWorkers = new HashMap<>();
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	protected void countDataAndAck(Tuple tuple) {
		if (startTime == 0) {
			startTime = DKGUtils.getCurrentTimestamp();
		}

		// update latestTimeConsumption
		updateTimeConsumption();

		String workerId = (String) tuple.getValueByField("workerId");
		String key = (String) tuple.getValueByField("key");
		Long count = (Long) tuple.getValueByField("count");
		//Long timestamp = (Long) tuple.getValueByField("timestamp");

		// aggregate new value
		distributionAggregator.aggregate(workerId, count);

		// aggregate total count
		totalCount += count;

		keyWorkers.putIfAbsent(key, new LinkedHashSet<>());
		Set<String> workerSet = keyWorkers.get(key);
		if (!workerSet.contains(workerId)) {
			workerSet.add(workerId);
			++totalWorkers;
		}
		collector.ack(tuple);
	}

	@Override
	protected void emitCurrentWindowAndAdvance() {
		if (totalCount <= latestTotalCount) {
			if (canPrintOut) {
				canPrintOut = false;
				printKeyWorkerCounts();
			}
			return;
		}

		canPrintOut = true;
		latestTotalCount = totalCount;
		long distinctKeys = keyWorkers.size();
//		long totalWorkers = keyWorkers.values().parallelStream().mapToLong(Set::size).sum();
		emitKeyWorkerCounts();
		calculateDistributionCost(distinctKeys, totalWorkers);
//		LOGGER.info("#KS: keyWorkers.size() = {}", keyWorkers.size());
		LOGGER.info("#KW: DistinctKeys: {}, TotalWorkers: {}", distinctKeys, totalWorkers);
	}

	private void updateTimeConsumption() {
		long currentTime = DKGUtils.getCurrentTimestamp();
		latestTimeConsumption = currentTime - startTime;
	}

	private long getTotalWorkers() {
		long totalWorkers = 0;
		for (Set<String> value : keyWorkers.values()) {
			int numberOfWorkers = value.size();
			totalWorkers += numberOfWorkers;
		}
		return totalWorkers;
	}

	private void emitKeyWorkerCounts() {
		long timestamp = DKGUtils.getCurrentTimestamp();
		for (Map.Entry<String, Set<String>> entry : keyWorkers.entrySet()) {
			String key = entry.getKey();
			int numberOfWorkers = entry.getValue().size();
			Message message = new Message(key, timestamp);
			message.addTag("key", key);
			message.addField("numberOfWorkers", numberOfWorkers);
			collector.emit(new Values(message.getKey(), message));

			if ("turkey".equalsIgnoreCase(key)) {
				LOGGER.info("#KWCT: {},{}", timestamp, numberOfWorkers);
			}
		}
	}

	private void printKeyWorkerCounts() {
		long numberOfTotalWorkers = 0;
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Set<String>> entry : keyWorkers.entrySet()) {
			String key = entry.getKey();
			int numberOfWorkers = entry.getValue().size();
			numberOfTotalWorkers += numberOfWorkers;
//			Message message = new Message(key, timestamp);
//			message.addTag("key", key);
//			message.addField("numberOfWorkers", numberOfWorkers);
			sb.append(key).append(",").append(numberOfWorkers).append("\n");
//			collector.emit(new Values(message.getKey(), message));
//			LOGGER.info("#KD: Key: {}, Workers: {}", key, numberOfWorkers);
		}
		sb.append("TotalWorkerCount: ").append(numberOfTotalWorkers);
		LOGGER.info("#KWC: Key,WorkerCount:\n{}", sb);
	}

	private void calculateDistributionCost(long distinctKeys, long totalWorkers) {
		double stdDev = distributionAggregator.stdDev();
		double distCost = distinctKeys > 0 ? totalWorkers / (double) distinctKeys : 0;
		double throughputRatio = (double) totalCount / (latestTimeConsumption / 1000);

		Message message = new Message("EVENT_INFO");
		message.addField("TOTAL_COUNT", totalCount);
		message.addField("DURATION", latestTimeConsumption);
		message.addField("STD_DEV", stdDev);
		message.addField("DIST_COST", distCost);
		message.addField("THROUGHPUT_RATIO", throughputRatio);
		collector.emit(new Values(message.getKey(), message));

		// log for observation
		String stdDevVal = String.format("%.4f", stdDev);
		String distCostVal = String.format("%.4f", distCost);
		String throughputRatioVal = String.format("%.0f", throughputRatio);
		LOGGER.info("#DIST: TotalCount: {}, Duration(ms): {}, StdDev: {}, DistCost: {}, ThroughputRatio(rec/sec): {}",
				totalCount, latestTimeConsumption, stdDevVal, distCostVal, throughputRatioVal);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
