package com.orhundalabasmaz.storm.loadBalancer.bolts;

import com.orhundalabasmaz.storm.loadBalancer.counter.CountryCounter;
import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerBolt extends BaseRichBolt {
	private OutputCollector collector;
	private CountryCounter counter;
	private long tickFrequencyInSeconds;
	private long processDuration;
	private long aggregationDuration;
//	private static final LoadMonitor loadMonitor = new LoadMonitor();

	public WorkerBolt(long tickFrequencyInSeconds, long processDuration, long aggregationDuration) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
		this.processDuration = processDuration;
		this.aggregationDuration = aggregationDuration;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		Logger.info("bolt# prepare: collector assigned");
		this.collector = outputCollector;
		this.counter = new CountryCounter(processDuration, aggregationDuration);
	}

	@Override
//	synchronized    // todo is necessary ?
	public void execute(Tuple tuple) {
		if (isTickTuple(tuple)) {
			handleLoadInfo();
			emitCurrentWindowCounts();
		} else {
			Logger.info("countDataAndAck");
			countDataAndAck(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		Logger.log("bolt# output field declared: " + "frequency");
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
		Logger.info("countDataAndAck by " + key);
		countKey(key);
		collector.ack(tuple);
	}

	private void countKey(String key) {
		counter.count(key);
	}

	private void emitCurrentWindowCounts() {
		Map<String, Integer> counts = counter.getCountsThenAdvanceWindow();
		emit(counts);
	}

	private void emit(Map<String, Integer> counts) {
		/*for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			String key = entry.getKey();
			Integer count = entry.getValue();
			String boltId = getBoltId();
			collector.emit(new Values(key, boltId, count));
			Logger.info("WorkerBolt: " + key + " - " + count);
		}*/
		String boltId = getBoltId();
		collector.emit(new Values(boltId, counts));
	}

	private void handleLoadInfo() {
		String boltId = getBoltId();
		Map<String, Integer> counts = counter.getCounts();
//		loadMonitor.load(boltId, counts);

		// Convert Map to List
		List<Map.Entry<String, Integer>> list = new LinkedList<>(counts.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
			                   Map.Entry<String, Integer> o2) {
				return (o1.getKey()).compareTo(o2.getKey());
			}
		});

		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("## CURRENT LOAD ON COUNTER BOLT ##").append("\n")
				.append("## Bolt: ").append(boltId).append("\n")
				.append("## Total counts: ").append(counts.size()).append("\n")
				.append("-------------------").append("\n");
		for (Map.Entry<String, Integer> entry : list) {
			sb.append("#  ").append(entry.getKey()).append(" - ").append(entry.getValue()).append("\n");
		}
		sb.append("\n");
//		Logger.log(sb.toString());
	}

	private String getBoltId() {
		return this.toString().split("@")[1];
	}
}
