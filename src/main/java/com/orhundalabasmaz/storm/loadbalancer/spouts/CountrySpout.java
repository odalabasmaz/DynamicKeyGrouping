package com.orhundalabasmaz.storm.loadbalancer.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.orhundalabasmaz.storm.utils.CustomLogger;
import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class CountrySpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random rand = new Random();

	private List<Values> valuesList;
	private StreamType streamType;
	//	private long sleepDuration = Configuration.TIME_INTERVAL_BETWEEN_DATA_STREAMS;
	private BufferedReader reader;
	private long startTime, endTime;
	private long keyCount = 0;
	private List<String> dataset = new LinkedList<>();
	private Iterator<String> iterator;

	public CountrySpout(StreamType streamType) {
		this.streamType = streamType;
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		CustomLogger.info("spout# open: collector assigned");
		this.collector = spoutOutputCollector;
		this.valuesList = Data.getValuesList();
		this.reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("data/data.txt")));
//		initDataset();  // if emitStaticData enabled
		this.startTime = System.currentTimeMillis();
	}

	private void initDataset() {
		String key;
		try {
			while ((key = reader.readLine()) != null) {
				dataset.add(key.trim());
			}
			iterator = dataset.iterator();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		DKGUtils.sleepInMicroseconds(100);
//		emitStaticData();
		emitSyntheticData();
	}

	private void emitStaticData() {
		if (!iterator.hasNext()) {
			System.exit(-1);
		}
		String key = iterator.next();
		keyCount++;
		CustomLogger.info("### KeyCount: " + keyCount + " " + DKGUtils.getCurrentDatetime());
		collector.emit(new Values(key));
	}

	private void emitSyntheticData() {
		switch (streamType) {
			case HOMOGENEOUS:
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
		CustomLogger.info("spout# emitting new value: " + valuesList.get(index));
		collector.emit(valuesList.get(index));
	}

	/* emit sequential data */
	private int seqIndex = 0;

	private void sequentialData() {
		seqIndex %= valuesList.size();
		int index = seqIndex++;
		CustomLogger.info("spout# emitting new value: " + valuesList.get(index));
		collector.emit(valuesList.get(index));
	}

	/* emit skewness data */
	private void skewnessData() {
		Values value;
		int randNum = rand.nextInt(100);
		if (randNum < 80) { //80
			value = new Values("turkey");
			CustomLogger.info("spout# emitting new value: " + value);
			collector.emit(value);
		} else if (randNum < 90) {
			value = new Values("spain");
			CustomLogger.info("spout# emitting new value: " + value);
			collector.emit(value);
		} else {
			randomData();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		CustomLogger.info("spout# output field declared: " + "spout");
		outputFieldsDeclarer.declare(new Fields("key"));
	}
}
