package com.orhundalabasmaz.storm.loadBalancer.spouts;

import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author Orhun Dalabasmaz
 */
public class CountrySpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random rand = new Random();

	private List<Values> valuesList;
	private final DataType DATA_TYPE = DataType.SKEW;
	private final long SLEEP_DURATION = 10L;

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		Logger.info("spout# open: collector assigned");
		this.collector = spoutOutputCollector;
		this.valuesList = Data.getValuesList();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(SLEEP_DURATION);
		switch (DATA_TYPE) {
			case HOMOGEN:
				sequentialData();
				break;
			case RANDOM:
				randomData();
				break;
			case SKEW:
				skewnessData();
				break;
			default:
				throw new UnsupportedOperationException("Unsupported data type");
		}
	}

	/* emit randomly data */
	private void randomData() {
		int index = rand.nextInt(valuesList.size());
		Logger.info("spout# emitting new value: " + valuesList.get(index));
		collector.emit(valuesList.get(index));
	}

	/* emit sequential data */
	private int seqIndex = 0;

	private void sequentialData() {
		seqIndex %= valuesList.size();
		int index = seqIndex++;
		Logger.info("spout# emitting new value: " + valuesList.get(index));
		collector.emit(valuesList.get(index));
	}

	/* emit skewness data */
	private void skewnessData() {
		Values value;
		int randNum = rand.nextInt(100);
		if (randNum < 80) { //80
			value = new Values("turkey");
			Logger.info("spout# emitting new value: " + value);
			collector.emit(value);
		} else if (randNum < 90) {
			value = new Values("spain");
			Logger.info("spout# emitting new value: " + value);
			collector.emit(value);
		} else {
			randomData();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		Logger.info("spout# output field declared: " + "spout");
		outputFieldsDeclarer.declare(new Fields("country"));
	}
}
