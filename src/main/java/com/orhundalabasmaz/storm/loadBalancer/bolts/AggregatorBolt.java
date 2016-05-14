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
public class AggregatorBolt extends BaseRichBolt {
	private OutputCollector collector;
	private CountryCounter countryCounter;
	private int tickFrequencyInSeconds;

	public AggregatorBolt(int tickFrequencyInSeconds) {
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
			emitCurrentWindowCounts();
		} else {
			Logger.info("countDataAndAck");
			countDataAndAck(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		Logger.log("bolt# output field declared: " + "frequency");
		outputFieldsDeclarer.declare(new Fields("result"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		final Map<String, Object> conf = new HashMap<>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

	private static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	private void countDataAndAck(Tuple tuple) {
		String country = tuple.getString(0);
		Integer count = tuple.getInteger(1);
		Logger.info("countDataAndAck by " + country + " - " + count);
		addCountry(country, count);
		collector.ack(tuple);
	}

	private void addCountry(String country, Integer count) {
		countryCounter.count(country, count);
	}

	private void emitCurrentWindowCounts() {
//		final Map<String, Integer> counts = countryCounter.getCountsThenAdvanceWindow();
		Map<String, Integer> counts = countryCounter.getCounts();
		emit(counts);
	}

	private void emit(Map<String, Integer> counts) {
		// Convert Map to List
		List<Map.Entry<String, Integer>> list = new LinkedList<>(counts.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
			                   Map.Entry<String, Integer> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		StringBuilder sb = new StringBuilder();
		sb.append("RESULTS :>").append("\n");

		int count = 1;
		for (Map.Entry<String, Integer> entry : list) {
			sb.append("#")
					.append(count++)
					.append(": ")
					.append(entry.getKey())
					.append(" - ")
					.append(entry.getValue())
					.append("\n");
		}

		collector.emit(new Values(sb.toString()));
	}

	/*private void emit(Map<String, Integer> counts) {
		for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			String country = entry.getKey();
			Integer count = entry.getValue();
			String result = country + " - " + count;
			collector.emit(new Values(result));
		}
	}*/
}
