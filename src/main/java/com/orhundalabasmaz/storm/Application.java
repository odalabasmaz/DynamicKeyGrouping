package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.common.Topology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.loadbalancer.LoadBalancerTopology;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadbalancer.spouts.StreamType;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import com.orhundalabasmaz.storm.utils.Logger;

/**
 * @author Orhun Dalabasmaz
 */
public class Application {

	public static void main(String... args) {
		String testId = DKGUtils.generateTestId();
		String dataSet = "COUNTRY";
		long processDuration = 10;
		long terminationDuration = 300_000 * 10L;
		GroupingType groupingType = GroupingType.DYNAMIC_KEY;
		StreamType streamType = StreamType.SKEW;
		int numberOfWorkers = 1;
		int numberOfSpouts = 1;
		int numberOfSplitterBolts = 10;
		int numberOfWorkerBolts = 10;
		int numberOfAggregatorBolts = 10;
		int numberOfOutputBolts = 1;
		int retryCount = 1;
		StormMode stormMode = StormMode.LOCAL;
		String sourceName = "source1";
		String sinkName = "sink1";
		String IPAddr = "85.110.34.250"; //"localhost"

		Logger.log("begin...");
		Configuration config =
				ConfigurationBuilder.getInstance()
						.defaultSet()
						.testId(testId)
						.dataSet(dataSet)
						.processDuration(processDuration)
						.terminationDuration(terminationDuration)
						.groupingType(groupingType)
						.streamType(streamType)
						.numberOfWorkers(numberOfWorkers)
						.numberOfSpouts(numberOfSpouts * numberOfWorkers)
						.numberOfSplitterBolts(numberOfSplitterBolts * numberOfWorkers)
						.numberOfWorkerBolts(numberOfWorkerBolts * numberOfWorkers)
						.numberOfAggregatorBolts(numberOfAggregatorBolts * numberOfWorkers)
						.numberOfOutputBolts(numberOfOutputBolts * numberOfWorkers)
						.retryCount(retryCount)
						.stormMode(stormMode)
						.sourceName(sourceName)
						.sinkName(sinkName)
						.ipAddr(IPAddr)
						.build();
		execute(config);
		Logger.log("end...");
	}

	private static void execute(Configuration config) {
		Logger.log("test begins... " + DKGUtils.getCurrentDatetime());
		int retryCount = config.getRetryCount();
		for (int i = 1; i <= retryCount; ++i) {
			Logger.log("test #" + i + " - running...");
			run(config);
			Logger.log("test #" + i + " - done.");
		}
		Logger.log("test ends... " + DKGUtils.getCurrentDatetime());
	}

	private static void run(Configuration runtimeConf) {
		Topology topology = new LoadBalancerTopology(runtimeConf);
		topology.init();
		topology.run();
	}
}
