package com.orhundalabasmaz.storm.common;

import backtype.storm.grouping.CustomStreamGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadbalancer.grouping.KeyGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.PartialKeyGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.ShuffleGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.dkg.DynamicKeyGrouping;

/**
 * @author Orhun Dalabasmaz
 */
public class StreamingGroupFactory {

	private StreamingGroupFactory() {
	}

	public static StreamingGroupFactory getInstance() {
		return new StreamingGroupFactory();
	}

	public CustomStreamGrouping getStreamGrouping(GroupingType groupingType) {
		CustomStreamGrouping streamGrouping;
		switch (groupingType) {
			case SHUFFLE:
//				counterDeclarer.shuffleGrouping(splitterBoltName);
				streamGrouping = new ShuffleGrouping();
				break;
			case KEY:
//				counterDeclarer.fieldsGrouping(splitterBoltName, new Fields(dataKey));
				streamGrouping = new KeyGrouping();
				break;
			case PARTIAL_KEY:
				streamGrouping = new PartialKeyGrouping();
				break;
			case DYNAMIC_KEY:
				streamGrouping = new DynamicKeyGrouping();
				break;
			default:
				throw new UnsupportedOperationException("Unexpected groupingType: " + groupingType);
		}
		return streamGrouping;
	}
}
