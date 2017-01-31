package com.orhundalabasmaz.storm.loadbalancer.bolts.old;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.orhundalabasmaz.storm.utils.CustomLogger;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class OutputResultsBolt extends BaseRichBolt {

	public OutputResultsBolt() {
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		CustomLogger.log("bolt# prepare: collector assigned");
	}

	@Override
	public void execute(Tuple tuple) {
//		CustomLogger.log(":: FINAL :: \n" + tuple.getString(0));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		CustomLogger.log("bolt# outputs the result");
	}
}
