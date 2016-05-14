package com.orhundalabasmaz.storm.loadBalancer.bolts;

import com.orhundalabasmaz.storm.loadBalancer.counter.CountryCounter;
import com.orhundalabasmaz.storm.loadBalancer.monitoring.LoadMonitor;
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
public class CounterBolt extends BaseRichBolt {
	private OutputCollector collector;
	private CountryCounter countryCounter;
	private int tickFrequencyInSeconds;

	private static final LoadMonitor loadMonitor = new LoadMonitor();

	public CounterBolt(int tickFrequencyInSeconds) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		Logger.info("bolt# prepare: collector assigned");
		this.collector = outputCollector;
		this.countryCounter = new CountryCounter();
	}

	@Override
	synchronized    // todo is necessary ?
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
		outputFieldsDeclarer.declare(new Fields("country", "count"));
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
		String country = tuple.getString(0);
		Logger.info("countDataAndAck by " + country);
		addCountry(country);
		collector.ack(tuple);
	}

	private void addCountry(String country) {
		countryCounter.count(country);
	}

	private void emitCurrentWindowCounts() {
		Map<String, Integer> counts = countryCounter.getCountsThenAdvanceWindow();
		emit(counts);
	}

	private void emit(Map<String, Integer> counts) {
		for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			final String country = entry.getKey();
			final Integer count = entry.getValue();
			collector.emit(new Values(country, count));
			Logger.info("CounterBolt: " + country + " - " + count);
		}
	}

	private void handleLoadInfo() {
		String boltId = this.toString().split("@")[1];
		Map<String, Integer> counts = countryCounter.getCounts();
		loadMonitor.load(boltId, counts);

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
				.append("## CURRENET LOAD ON COUNTER BOLT ##").append("\n")
				.append("## Bolt: ").append(boltId).append("\n")
				.append("## Total counts: ").append(counts.size()).append("\n")
				.append("-------------------").append("\n");
		for (Map.Entry<String, Integer> entry : list) {
			sb.append("#  ").append(entry.getKey()).append(" - ").append(entry.getValue()).append("\n");
		}
		sb.append("\n");
		Logger.log(sb.toString());
	}

}
