package com.orhundalabasmaz.storm.loadBalancer.spouts;

import com.orhundalabasmaz.storm.loadBalancer.Configuration;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;
import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Orhun Dalabasmaz
 */
public class CountrySpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random rand = new Random();

	private List<Values> valuesList;
	private final DataType DATA_TYPE = Configuration.DATA_TYPE;
	private final long SLEEP_DURATION = Configuration.TIME_INTERVAL_BETWEEN_DATA_STREAMS;
	private BufferedReader reader;
	private long startTime, endTime;
	private long keyCount = 0;
	private List<String> dataset = new LinkedList<>();
	private Iterator<String> iterator;
	private DateFormat formatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		Logger.info("spout# open: collector assigned");
		this.collector = spoutOutputCollector;
		this.valuesList = Data.getValuesList();
		this.reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("data.txt")));
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
		Logger.info("### KeyCount: " + keyCount + " " + formatter.format(new Date()));
		collector.emit(new Values(key));
	}

	private void emitSyntheticData() {
		switch (DATA_TYPE) {
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
		outputFieldsDeclarer.declare(new Fields("key"));
	}
}
