package com.orhundalabasmaz.storm.loadBalancer.bolts;

import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by orhun on 17.10.2015.
 */
public class SplitterBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		Logger.log("bolt# prepare: collector assigned");
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		String country = tuple.getString(0);
		Logger.info("bolt# emitting new value: " + country);
		collector.emit(tuple, new Values(country));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		Logger.log("bolt# output field declared: " + "splitter");
		outputFieldsDeclarer.declare(new Fields("country"));
	}
}
