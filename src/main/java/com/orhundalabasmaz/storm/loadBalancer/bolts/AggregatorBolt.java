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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class AggregatorBolt extends BaseRichBolt {
	private OutputCollector collector;
	private CountryCounter counter;
	private int tickFrequencyInSeconds;

	private LoadMonitor loadMonitor = new LoadMonitor();
	private DateFormat formatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
	private long keyCount;
	private long startTime;
	private int timeDurationFactor, keyCountFactor;

	public AggregatorBolt(int tickFrequencyInSeconds) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		Logger.info("bolt# prepare: collector assigned");
		this.collector = outputCollector;
		this.counter = new CountryCounter();
		this.keyCount = 0;
		this.timeDurationFactor = 1;
		this.keyCountFactor = 1;
		this.startTime = System.currentTimeMillis();
	}

	@Override
	//synchronized    // todo is necessary ?
	public void execute(Tuple tuple) {
		if (isTickTuple(tuple)) {
			emitCurrentWindowCounts();
		} else {
			Logger.info("countDataAndAck");
			countDataAndAck(tuple);
		}
		checkLatencyAndThroughput();
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

	@SuppressWarnings("unchecked")
	private void countDataAndAck(Tuple tuple) {
//		String key = tuple.getString(0);
//		String boltId = tuple.getString(1);
//		Integer count = tuple.getInteger(2);
//		Logger.info("countDataAndAck by " + key + " - " + count + " via bolt: " + boltId);
		String boltId = tuple.getString(0);
		Map<String, Integer> counts = (Map<String, Integer>) tuple.getValue(1);
		aggregateValues(counts);
//		aggregateValue(key, count);
		handleLoadInfo(boltId, counts);
		collector.ack(tuple);
	}

	private void aggregateValues(Map<String, Integer> counts) {
		for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			String key = entry.getKey();
			Integer count = entry.getValue();
			aggregateValue(key, count);
		}
	}

	private void aggregateValue(String key, Integer count) {
		counter.count(key, count);
		keyCount += count;
	}

	private void emitCurrentWindowCounts() {
//		final Map<String, Integer> counts = countryCounter.getCountsThenAdvanceWindow();
		Map<String, Integer> counts = counter.getCounts();
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

	private void handleLoadInfo(String boltId, Map<String, Integer> counts) {
//		Map<String, Integer> counts = counter.getCounts();
		loadMonitor.load(boltId, counts);
	}

	private void checkLatencyAndThroughput() {
		long endTime = System.currentTimeMillis();
		long timeDuration = endTime - startTime;
		if (timeDuration >= 100_000 * timeDurationFactor) {
			++timeDurationFactor;
			checkOut(keyCount, timeDuration);
		}
		if (keyCount >= 100_000 * keyCountFactor) {
			++keyCountFactor;
			checkOut(keyCount, timeDuration);
		}
	}

	private void checkOut(long keyCount, long timeDuration) {
		String date = formatter.format(new Date());
		System.out.println("## " + date + " ## Emitted " + keyCount + " keys in " + timeDuration + " ms");

		if (timeDuration >= 5 * 60 * 1000) {
			System.out.println("#### TERMINATING ####");
			System.exit(-1);
		}
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
