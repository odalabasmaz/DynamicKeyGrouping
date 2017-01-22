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
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.CountryAggregator;
import com.orhundalabasmaz.storm.loadbalancer.monitoring.LoadMonitor;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import com.orhundalabasmaz.storm.utils.Logger;
import com.orhundalabasmaz.storm.utils.ResultBuilder;
import com.orhundalabasmaz.storm.utils.ResultLogger;

import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class AggregatorBoltOld extends BaseRichBolt {
	private OutputCollector collector;
	private CountryAggregator counter;
	private long tickFrequencyInSeconds;
	private Configuration runtimeConf;

	private String testId;
	private long keyCount;
	private long startTime;
	private long checkTimeInterval;
	private int timeDurationFactor, keyCountFactor;
	private LoadMonitor loadMonitor;
	private ResultLogger resultLogger;
	private boolean enabled;

	public AggregatorBoltOld(Configuration runtimeConf) {
		this.runtimeConf = runtimeConf;
		this.testId = runtimeConf.getTestId();
		this.loadMonitor = new LoadMonitor(runtimeConf.isLogEnabled(), runtimeConf.getNumberOfWorkerBolts());
		this.tickFrequencyInSeconds = runtimeConf.getTimeIntervalOfAggregatorBolts();
		this.checkTimeInterval = runtimeConf.getTimeIntervalOfCheck();
		this.resultLogger = new ResultLogger("results.csv");
		this.enabled = true;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		Logger.info("bolt# prepare: collector assigned");
		this.collector = outputCollector;
//		this.counter = new CountryAggregator(runtimeConf.getProcessDuration(), runtimeConf.getAggregationDuration());
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
//		Long count = tuple.getLong(2);
//		Logger.info("countDataAndAck by " + key + " - " + count + " via bolt: " + boltId);
		String boltId = tuple.getString(0);
		Map<String, Long> counts = (Map<String, Long>) tuple.getValue(1);
		aggregateValues(counts);
//		aggregateValue(key, count);
		handleLoadInfo(boltId, counts);
		collector.ack(tuple);
	}

	private void aggregateValues(Map<String, Long> counts) {
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			aggregateValue(key, count);
		}
	}

	private void aggregateValue(String key, Long count) {
//		counter.count(key, count);
		keyCount += count;
	}

	private void emitCurrentWindowCounts() {
//		final Map<String, Long> counts = countryCounter.getCountsThenAdvanceWindow();
		Map<String, Long> counts = counter.getCounts();
		emit(counts);
	}

	private void emit(Map<String, Long> counts) {
		// Convert Map to List
		List<Map.Entry<String, Long>> list = new LinkedList<>(counts.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1,
							   Map.Entry<String, Long> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		StringBuilder sb = new StringBuilder();
		sb.append("RESULTS :>").append("\n");

		int count = 1;
		for (Map.Entry<String, Long> entry : list) {
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

	private void handleLoadInfo(String boltId, Map<String, Long> counts) {
//		Map<String, Long> counts = counter.getCounts();
		loadMonitor.load(boltId, counts);
	}

	private void checkLatencyAndThroughput() {
		long endTime = System.currentTimeMillis();
		long timeDuration = endTime - startTime;
		if (timeDuration >= checkTimeInterval * timeDurationFactor) {
			++timeDurationFactor;
			checkOut(keyCount, timeDuration);
		}
		/*if (keyCount >= 10_000 * keyCountFactor) {
			++keyCountFactor;
			checkOut(keyCount, timeDuration);
		}*/
	}

	private void checkOut(long keyCount, long timeDuration) {
		if (!enabled || timeDuration < runtimeConf.getTerminationDuration()) {
			return;
		}
		enabled = false;
		String datetime = DKGUtils.getCurrentDatetime();
		Logger.log("#### TERMINATING #### (" + datetime + ")\n" +
				"## Emitted " + keyCount + " keys in " + timeDuration + " ms" + "\n" +
				"## Memory Consumption ## " + loadMonitor.getMemoryConsumptionInfo());
		DKGUtils.beep();

		double throughputRatio = timeDuration > 0 ? (double) 1000 * keyCount / timeDuration : 0;
		String resultValue = ResultBuilder.getInstance()
				.testId(testId)
				.datetime(datetime)
				.groupingType(runtimeConf.getGroupingType())
				.dataSet(runtimeConf.getDataSet())
				.streamType(runtimeConf.getStreamType())
				.processDuration(runtimeConf.getProcessDuration())
				.aggregationDuration(runtimeConf.getAggregationDuration())
				.numberOfSpouts(runtimeConf.getNumberOfSpouts())
				.numberOfWorkerBolts(runtimeConf.getNumberOfWorkerBolts())
				.latency(timeDuration)
				.throughput(keyCount)
				.throughputRatio(DKGUtils.formatDoubleValue(throughputRatio))
				.numberOfDistinctKeys(loadMonitor.getNumberOfDistinctKeys())
				.numberOfConsumedKeys(loadMonitor.getNumberOfConsumedKeys())
				.build();
		resultLogger.log(resultValue);
	}

	/*private void emit(Map<String, Long> counts) {
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String country = entry.getKey();
			Long count = entry.getValue();
			String result = country + " - " + count;
			collector.emit(new Values(result));
		}
	}*/
}
