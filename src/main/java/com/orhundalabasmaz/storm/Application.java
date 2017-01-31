package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.common.Topology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.loadbalancer.LoadBalancerTopology;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadbalancer.spouts.StreamType;
import com.orhundalabasmaz.storm.utils.CustomLogger;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Orhun Dalabasmaz
 */
public class Application {
	private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

	private Application() {
	}

	/**
	 * $ java -jar dkg-wd.jar SHUFFLE|KEY|PARTIAL_KEY|DYNAMIC_KEY 10 1
	 * */
	public static void main(String... args) {
		if (args.length < 3) {
			LOGGER.error("groupingType, workerCount and processDuration must be specified!\n" +
					"i.e. $ java -jar dkg-wd.jar DYNAMIC_KEY 50 1");
			throw new UnsupportedOperationException("groupingType, workerCount and processDuration must be specified!");
		}

		GroupingType groupingType = GroupingType.valueOf(args[0]);
		int numberOfWorkerBolts = Integer.parseInt(args[1]);
		long processDuration = Long.parseLong(args[2]);

		String testId = DKGUtils.generateTestId();
		String dataSet = "COUNTRY";
//		long processDuration = 1;
		long terminationDuration = 1000 * 60 * 1000L;
//		GroupingType groupingType = GroupingType.DYNAMIC_KEY;
		StreamType streamType = StreamType.SKEW;
		int numberOfWorkers = 1;
		int numberOfSpouts = 5;
		int numberOfSplitterBolts = 10;
//		int numberOfWorkerBolts = 10;
		int numberOfAggregatorBolts = 10;
		int numberOfOutputBolts = 1;
		int retryCount = 1;
		StormMode stormMode = StormMode.LOCAL;
		String sourceName = "source-country";
		String sinkName = "sink1";
		String IPAddr = "localhost"; //"localhost 85.110.34.250"

		CustomLogger.log("begin...");
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
		CustomLogger.log("end...");
	}

	private static void execute(Configuration config) {
		CustomLogger.log("test begins... " + DKGUtils.getCurrentDatetime());
		int retryCount = config.getRetryCount();
		for (int i = 1; i <= retryCount; ++i) {
			CustomLogger.log("test #" + i + " - running...");
			run(config);
			CustomLogger.log("test #" + i + " - done.");
		}
		CustomLogger.log("test ends... " + DKGUtils.getCurrentDatetime());
	}

	private static void run(Configuration runtimeConf) {
		Topology topology = new LoadBalancerTopology(runtimeConf);
		topology.init();
		topology.run();
	}
}
