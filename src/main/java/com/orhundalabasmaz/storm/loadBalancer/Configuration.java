package com.orhundalabasmaz.storm.loadBalancer;

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

	public static final long TOPOLOGY_TIMEOUT = 10 * 60 * 1000;
}
