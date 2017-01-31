package com.orhundalabasmaz.storm.cucumber;

/*import com.orhundalabasmaz.storm.common.Topology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.config.RuntimeConfig;
import com.orhundalabasmaz.storm.loadbalancer.LoadBalancerTopology;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadbalancer.spouts.StreamType;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import com.orhundalabasmaz.storm.utils.CustomLogger;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.List;*/

/**
 * @author Orhun Dalabasmaz
 */
/*
public class StepDefinitions {
	private String testId;
	private ConfigurationBuilder configBuilder;

	@Before
	public void initialize() {
		CustomLogger.log("scenario initializing...");
		testId = DKGUtils.generateTestId();
		configBuilder = ConfigurationBuilder.getInstance();
	}

	@Given("^Data type and process duration$")
	public void dataTypeAndProcessDuration(List<RuntimeConfig> runtimeConfigList) throws Throwable {
		RuntimeConfig config = runtimeConfigList.get(0);
		CustomLogger.log("dataSet is: " + config.getDataSet() +
				", process duration is: " + config.getProcessDuration() +
				", termination duration is: " + config.getTerminationDuration());
		configBuilder
				.defaultSet()
				.testId(testId)
				.dataSet(config.getDataSet())
				.processDuration(config.getProcessDuration())
				.terminationDuration(config.getTerminationDuration());
	}

	@When("^Grouping type is (\\w+)$")
	public void groupingType(GroupingType groupingType) throws Throwable {
		CustomLogger.log("methodType is: " + groupingType);
		configBuilder.groupingType(groupingType);
	}

	@And("^Stream type is (\\w+)$")
	public void streamType(StreamType streamType) throws Throwable {
		CustomLogger.log("streamType is: " + streamType);
		configBuilder.streamType(streamType);
	}

	@And("^Spout count is (\\d+)$")
	public void spoutCount(Integer spoutCount) throws Throwable {
		CustomLogger.log("spoutCount is: " + spoutCount);
		configBuilder.numberOfSpouts(spoutCount);
	}

	@And("^WorkerBolt count is (\\d+)$")
	public void workerCount(Integer workerCount) throws Throwable {
		CustomLogger.log("workerCount is: " + workerCount);
		configBuilder.numberOfWorkerBolts(workerCount);
	}

	@Then("^Execute test$")
	public void executeTest(List<RuntimeConfig> runtimeConfigList) throws Throwable {
		CustomLogger.log("test begins... " + DKGUtils.getCurrentDatetime());
		int retryCount = runtimeConfigList.get(0).getRetryCount();
		Configuration config = configBuilder.build();
		for (int i = 1; i <= retryCount; ++i) {
			try {
				CustomLogger.log("test #" + i + " - running...");
				Topology topology = new LoadBalancerTopology(StormMode.CLUSTER, config);
				topology.init();
				topology.run();
			} catch (Exception e) {
				CustomLogger.log("test #" + i + " - Exception occurred: " + e.getMessage());
				e.printStackTrace();
			} finally {
				CustomLogger.log("test #" + i + " - done.");
			}
		}
		CustomLogger.log("test ends... " + DKGUtils.getCurrentDatetime());
	}

	@And("^Test successfully completed$")
	public void testSuccessfullyCompleted() throws Throwable {
		CustomLogger.log("successfully completed.");
	}

	@After
	public void cleanUp() {
		CustomLogger.log("scenario cleaning up.");
		CustomLogger.log("waiting for 5 secs.");
		DKGUtils.sleepInSeconds(5);
	}
}
*/
