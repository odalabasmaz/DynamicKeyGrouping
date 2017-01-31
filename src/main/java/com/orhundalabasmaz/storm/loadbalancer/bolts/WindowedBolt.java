package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.orhundalabasmaz.storm.utils.CustomLogger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class WindowedBolt extends BaseRichBolt {
	private long tickFrequencyInSeconds;

	protected WindowedBolt(long tickFrequencyInSeconds) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
	}

	@Override
	public final void execute(Tuple tuple) {
		if (isTickTuple(tuple)) {
			emitCurrentWindowAndAdvance();
		} else {
			CustomLogger.info("countDataAndAck");
			countDataAndAck(tuple);
		}
	}

	protected abstract void emitCurrentWindowAndAdvance();

	protected abstract void countDataAndAck(Tuple tuple);

	@Override
	public final Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

	private boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	protected final String getWorkerId() {
		return this.toString().split("@")[1].toUpperCase();
	}
}
