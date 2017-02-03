package com.orhundalabasmaz.storm.loadbalancer.bolts.observer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.model.Message;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerObserverBolt extends BaseRichBolt {
	private transient OutputCollector collector;
	private final List<String> workerIds = new ArrayList<>();

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized void execute(Tuple tuple) {
		String workerId = convertWorkerId((String) tuple.getValueByField("workerId"));
		String key = (String) tuple.getValueByField("key");
		Long count = (Long) tuple.getValueByField("count");
		Long timestamp = (Long) tuple.getValueByField("timestamp");

		Message message = new Message(key, timestamp);
		message.addTag("country", key);
		message.addTag("workerId", workerId);
		message.addField("count", count);
		//todo tuple,
		collector.emit(tuple, new Values(message.getKey(), message));
		collector.ack(tuple);
	}

	private String convertWorkerId(String workerId) {
		synchronized (workerIds) {
			if (!workerIds.contains(workerId)) {
				workerIds.add(workerId);
			}
		}
		String workerNo = String.valueOf(workerIds.indexOf(workerId) + 1);
		workerNo = StringUtils.leftPad(workerNo, 3, "0");
		return "Worker-" + workerNo;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("key", "message"));
	}
}
