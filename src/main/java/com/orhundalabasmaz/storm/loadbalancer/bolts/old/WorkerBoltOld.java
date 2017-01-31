package com.orhundalabasmaz.storm.loadbalancer.bolts.old;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.CountryAggregator;
import com.orhundalabasmaz.storm.utils.CustomLogger;

import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerBoltOld extends BaseRichBolt {
	private OutputCollector collector;
	private CountryAggregator counter;
	private long tickFrequencyInSeconds;
	private long processDuration;
	private long aggregationDuration;
//	private static final LoadMonitor loadMonitor = new LoadMonitor();

	public WorkerBoltOld(long tickFrequencyInSeconds, long processDuration, long aggregationDuration) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
		this.processDuration = processDuration;
		this.aggregationDuration = aggregationDuration;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		CustomLogger.info("bolt# prepare: collector assigned");
		this.collector = outputCollector;
//		this.counter = new CountryAggregator(processDuration, aggregationDuration);
	}

	@Override
//	synchronized    // todo is necessary ?
	public void execute(Tuple tuple) {
		if (isTickTuple(tuple)) {
			handleLoadInfo();
			emitCurrentWindowCounts();
		} else {
			CustomLogger.info("countDataAndAck");
			countDataAndAck(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		CustomLogger.log("bolt# output field declared: " + "frequency");
//		outputFieldsDeclarer.declare(new Fields("country", "boltId", "count"));
		outputFieldsDeclarer.declare(new Fields("boltId", "counts"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

	private static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	private void countDataAndAck(Tuple tuple) {
		String key = tuple.getString(0);
		CustomLogger.info("countDataAndAck by " + key);
		countKey(key);
		collector.ack(tuple);
	}

	private void countKey(String key) {
//		counter.count(key);
	}

	private void emitCurrentWindowCounts() {
		Map<String, Long> counts = counter.getCountsThenAdvanceWindow();
		emit(counts);
	}

	private void emit(Map<String, Long> counts) {
		/*for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			String boltId = getBoltId();
			collector.emit(new Values(key, boltId, count));
			CustomLogger.info("WorkerBolt: " + key + " - " + count);
		}*/
		String boltId = getBoltId();
		collector.emit(new Values(boltId, counts));
	}

	private void handleLoadInfo() {
		String boltId = getBoltId();
		Map<String, Long> counts = counter.getCounts();
//		loadMonitor.load(boltId, counts);

		// Convert Map to List
		List<Map.Entry<String, Long>> list = new LinkedList<>(counts.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1,
			                   Map.Entry<String, Long> o2) {
				return (o1.getKey()).compareTo(o2.getKey());
			}
		});

		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("## CURRENT LOAD ON COUNTER BOLT ##").append("\n")
				.append("## Bolt: ").append(boltId).append("\n")
				.append("## Total counts: ").append(counts.size()).append("\n")
				.append("-------------------").append("\n");
		for (Map.Entry<String, Long> entry : list) {
			sb.append("#  ").append(entry.getKey()).append(" - ").append(entry.getValue()).append("\n");
		}
		sb.append("\n");
//		CustomLogger.log(sb.toString());
	}

	private String getBoltId() {
		return this.toString().split("@")[1];
	}
}
