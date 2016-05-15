package com.orhundalabasmaz.storm.loadBalancer;

import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.DataType;

/**
 * @author Orhun Dalabasmaz
 */
public class Configuration {
	// NUMBER OF PROCESS UNITS
	public static final int N_WORKERS = 1;
	public static final int N_SPOUTS = 2;
	public static final int N_SPLITTER_BOLTS = 3;
	public static final int N_COUNTER_BOLTS = 4;
	public static final int N_AGGREGATOR_BOLTS = 1;
	public static final int N_RESULT_BOLTS = 1;
	public static final int N_TASKS = 2;

	// TIME INTERVAL
	public static final int T_COUNTER_BOLTS = 3;
	public static final int T_AGGREGATOR_BOLTS = 5;

	public static final long TOPOLOGY_TIMEOUT = 10 * 60 * 1000;         // ms
	public static final long TIME_INTERVAL_BETWEEN_DATA_STREAMS = 10;   // ms

	public static final DataType DATA_TYPE = DataType.SKEW;
	public static final GroupingType GROUPING_TYPE = GroupingType.DYNAMIC_KEY;
	public static final AggregatorType AGGREGATOR_TYPE = AggregatorType.CUMULATIVE;
}
