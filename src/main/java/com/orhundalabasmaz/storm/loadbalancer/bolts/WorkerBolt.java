package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.Aggregator;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.CountryAggregator;
import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerBolt extends WindowedBolt {
	private transient OutputCollector collector;
	private transient Aggregator aggregator;
	private long processDuration;
	private long aggregationDuration;

	public WorkerBolt(long tickFrequencyInSeconds, long processDuration, long aggregationDuration) {
		super(tickFrequencyInSeconds);
		this.processDuration = processDuration;
		this.aggregationDuration = aggregationDuration;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.aggregator = new CountryAggregator(aggregationDuration);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("workerId", "counts"));
	}

	@Override
	public void countDataAndAck(Tuple tuple) {
		String key = tuple.getString(0);
		doToughJob();
		aggregator.aggregate(key);
		collector.ack(tuple);
	}

	@Override
	public void emitCurrentWindowAndAdvance() {
		Map<String, Long> counts = aggregator.getCountsThenAdvanceWindow();
		String workerId = getWorkerId();
//		Integer numberOfDistinctKeys = counts.keySet().size();
		collector.emit(new Values(workerId, counts));
	}

	private void doToughJob() {
		DKGUtils.sleepInMilliseconds(processDuration);
	}
}
