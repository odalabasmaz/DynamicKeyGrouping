package com.orhundalabasmaz.storm.loadBalancer.bolts;

import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class OutputResultsBolt extends BaseRichBolt {

	public OutputResultsBolt() {
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		Logger.log("bolt# prepare: collector assigned");
	}

	@Override
	public void execute(Tuple tuple) {
//		Logger.log(":: FINAL :: \n" + tuple.getString(0));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		Logger.log("bolt# outputs the result");
	}
}
