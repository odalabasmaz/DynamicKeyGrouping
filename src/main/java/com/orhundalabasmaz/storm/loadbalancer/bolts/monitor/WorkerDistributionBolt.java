package com.orhundalabasmaz.storm.loadbalancer.bolts.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.loadbalancer.aggregator.AggregationDetail;
import com.orhundalabasmaz.storm.model.WorkerDistributionMessage;

import java.util.Map;
import java.util.Set;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerDistributionBolt extends BaseRichBolt {
	private transient OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
		AggregationDetail detail = (AggregationDetail) tuple.getValueByField("aggregationDetail");
		Long timestamp = (Long) tuple.getValueByField("timestamp");
		for (Map.Entry<String, Long> entry : detail.getWorkerCounts().entrySet()) {
			String workerId = entry.getKey();
			Long totalCount = entry.getValue();

			WorkerDistributionMessage message = new WorkerDistributionMessage(workerId, timestamp);
			message.setWorkerId(workerId);
			message.setTotalCount(totalCount);
			collector.emit(tuple, new Values(message.getKey(), message));
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
