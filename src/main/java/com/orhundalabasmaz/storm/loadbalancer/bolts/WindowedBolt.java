package com.orhundalabasmaz.storm.loadbalancer.bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.orhundalabasmaz.storm.common.ObjectObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class WindowedBolt extends BaseRichBolt implements ObjectObserver {
	private static final Logger LOGGER = LoggerFactory.getLogger(WindowedBolt.class);
	private long tickFrequencyInSeconds;

	protected WindowedBolt(long tickFrequencyInSeconds) {
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
	}

	@Override
	public final void execute(Tuple tuple) {
		if (isTickTuple(tuple)) {
			LOGGER.info("#WIN: {}", getObjectId());
			emitCurrentWindowAndAdvance();
		} else {
			LOGGER.debug("countDataAndAck: {}", tuple);
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

	private static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

}
