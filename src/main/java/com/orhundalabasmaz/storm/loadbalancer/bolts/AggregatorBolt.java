package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.AggregationDetail;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.Aggregator;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.CountryAggregator;
import com.orhundalabasmaz.storm.utils.Utils;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class AggregatorBolt extends WindowedBolt {
	private transient OutputCollector collector;
	private transient Aggregator aggregator;
	private long aggregationDuration;

	public AggregatorBolt(long tickFrequencyInSeconds, long aggregationDuration) {
		super(tickFrequencyInSeconds);
		this.aggregationDuration = aggregationDuration;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		this.aggregator = new CountryAggregator(aggregationDuration);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("aggregationDetail", "timestamp"));
	}

	@Override
	@SuppressWarnings("unchecked")
	public void countDataAndAck(Tuple tuple) {
		String workerId = (String) tuple.getValueByField("workerId");
		Map<String, Long> counts = (Map<String, Long>) tuple.getValueByField("counts");
		aggregator.aggregate(workerId, counts);
		collector.ack(tuple);
	}

	@Override
	public void emitCurrentWindowAndAdvance() {
		AggregationDetail detail = aggregator.getDetailedCountsThenAdvanceWindow();
		long timestamp = Utils.getCurrentTimestamp();
		collector.emit(new Values(detail, timestamp));
	}
}
