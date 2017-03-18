package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.common.SourceType;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.common.Topology;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.loadbalancer.LoadBalancerTopology;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.common.StreamType;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class Application {
	private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

	private Application() {
	}

	/**
	 * $ java -jar dkg-wd.jar SHUFFLE|KEY|PARTIAL_KEY|DYNAMIC_KEY 10 1
	 */
	public static void main(String... args) {
		if (args.length != 6) {
			LOGGER.error("stormMode, sourceType, groupingType, sourceName, numberOfSpouts and workerCount must be specified!\n" +
					"i.e. $ java -jar dkg-wd.jar LOCAL COUNTRY DYNAMIC_KEY source-country 5 10");
			throw new UnsupportedOperationException("stormMode, sourceType, groupingType, sourceName, numberOfSpouts and workerCount must be specified!");
		}

		StormMode stormMode = StormMode.valueOf(args[0]);
		SourceType sourceType = SourceType.valueOf(args[1]);
		GroupingType groupingType = GroupingType.valueOf(args[2]);
		String sourceName = args[3];
		int numberOfSpouts = Integer.parseInt(args[4]);
		int numberOfWorkerBolts = Integer.parseInt(args[5]);

		String testId = DKGUtils.generateTestId();
		long processDuration = 1L;
		long terminationDuration = 1000 * 60 * 1000L;
		StreamType streamType = StreamType.SKEW;
		int numberOfWorkers = 1;
		int numberOfSplitterBolts = 10;
		int numberOfAggregatorBolts = 10;
		int numberOfOutputBolts = 1;
		int retryCount = 1;
		String IPAddr = "192.168.1.39"; //localhost 78.165.170.40
		Map<String, String> groupingProps = new HashMap<>();
		groupingProps.put("distinctKeyCount", "30");

		LOGGER.info("begin...");
		Configuration config =
				ConfigurationBuilder.getInstance()
						.defaultSet()
						.testId(testId)
						.processDuration(processDuration)
						.terminationDuration(terminationDuration)
						.sourceType(sourceType)
						.groupingType(groupingType)
						.groupingProps(groupingProps)
						.streamType(streamType)
						.numberOfWorkers(numberOfWorkers)
						.numberOfSpouts(numberOfSpouts * numberOfWorkers)
						.numberOfSplitterBolts(numberOfSplitterBolts * numberOfWorkers)
						.numberOfWorkerBolts(numberOfWorkerBolts * numberOfWorkers)
						.numberOfAggregatorBolts(numberOfAggregatorBolts * numberOfWorkers)
						.numberOfOutputBolts(numberOfOutputBolts * numberOfWorkers)
						.timeIntervalOfWorkerBolts(10)
						.timeIntervalOfAggregatorBolts(60)
						.retryCount(retryCount)
						.stormMode(stormMode)
						.sourceName(sourceName)
						.ipAddr(IPAddr)
						.build();
		execute(config);
		LOGGER.info("end...");
	}

	private static void execute(Configuration config) {
		LOGGER.info("test begins... " + DKGUtils.getCurrentDatetime());
		int retryCount = config.getRetryCount();
		for (int i = 1; i <= retryCount; ++i) {
			LOGGER.info("test #{} - running...", i);
			run(config);
			LOGGER.info("test #{} - done.", i);
		}
		LOGGER.info("test ends... " + DKGUtils.getCurrentDatetime());
	}

	private static void run(Configuration runtimeConf) {
		Topology topology = new LoadBalancerTopology(runtimeConf);
		topology.init();
		topology.run();
	}
}
