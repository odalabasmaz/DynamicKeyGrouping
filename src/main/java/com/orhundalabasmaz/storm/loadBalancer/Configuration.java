package com.orhundalabasmaz.storm.loadBalancer;

import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.DataType;

/**
 * @author Orhun Dalabasmaz
 */
public class Configuration {
	// APP VERSION
	public static final String APP_VERSION = "v1.0";

	// NUMBER OF PROCESS UNITS
	public static final int N_WORKERS = 1;
	public static final int N_SPOUTS = 1;
	public static final int N_SPLITTER_BOLTS = 3;
	public static final int N_WORKER_BOLTS = 50;
	public static final int N_AGGREGATOR_BOLTS = 1;
	public static final int N_RESULT_BOLTS = 1;
	public static final int N_TASKS = 2;

	// TIME INTERVAL
	public static final int T_WORKER_BOLTS = 5;
	public static final int T_AGGREGATOR_BOLTS = 15;

	public static final long TERMINATION_TIMEOUT = 1 * 60 * 1000;         // ms
	public static final long TOPOLOGY_TIMEOUT = TERMINATION_TIMEOUT + 10000;         // ms
//	public static final long TOPOLOGY_TIMEOUT = 5 * 10 * 60 * 1000;         // ms
	public static final long TIME_INTERVAL_BETWEEN_DATA_STREAMS = 1;        // ms (default 1 ms)

	// RUNTIME PROPS
	public static final DataType DATA_TYPE = DataType.HOMOGENEOUS;
	public static final GroupingType GROUPING_TYPE = GroupingType.DYNAMIC_KEY;
	public static final AggregatorType AGGREGATOR_TYPE = AggregatorType.CUMULATIVE;
	public static final int PROCESS_DURATION = 10; //ms
	public static final int AGGREGATION_DURATION = 0; //ms
	public static final boolean LOG_ENABLED = true;

}
