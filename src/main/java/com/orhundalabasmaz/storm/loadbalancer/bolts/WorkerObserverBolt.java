package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.model.Message;
import com.orhundalabasmaz.storm.model.WorkersMessage;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerObserverBolt extends BaseRichBolt {
	private transient OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
		String workerId = (String) tuple.getValueByField("workerId");
		Map<String, Long> counts = (Map<String, Long>) tuple.getValueByField("counts");

		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			String country = entry.getKey();
			Long count = entry.getValue();
			Message message = new Message(country);
			message.addTag("country", country);
			message.addTag("workerId", workerId);
			message.addField("count", count);
			//todo tuple,
			collector.emit(tuple, new Values(message.getKey(), message));
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
