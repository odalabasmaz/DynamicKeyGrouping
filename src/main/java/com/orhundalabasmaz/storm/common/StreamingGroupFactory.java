package com.orhundalabasmaz.storm.common;

import backtype.storm.grouping.CustomStreamGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadbalancer.grouping.KeyGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.PartialKeyGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.ShuffleGrouping;
import com.orhundalabasmaz.storm.loadbalancer.grouping.dkg.DynamicKeyGrouping;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class StreamingGroupFactory {

	private StreamingGroupFactory() {
	}

	public static StreamingGroupFactory getInstance() {
		return new StreamingGroupFactory();
	}

	public CustomStreamGrouping getStreamGrouping(GroupingType groupingType, Map<String, String> groupingProps) {
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
				int distinctKeyCount = Integer.parseInt(groupingProps.get("distinctKeyCount"));
				streamGrouping = new DynamicKeyGrouping(distinctKeyCount);
				break;
			default:
				throw new UnsupportedOperationException("Unexpected groupingType: " + groupingType);
		}
		return streamGrouping;
	}
}
