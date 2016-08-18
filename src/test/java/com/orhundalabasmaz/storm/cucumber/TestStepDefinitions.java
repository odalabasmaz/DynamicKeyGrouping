package com.orhundalabasmaz.storm.cucumber;

import com.orhundalabasmaz.storm.TestLoadBalancerTopology;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;
import com.orhundalabasmaz.storm.loadBalancer.spouts.StreamType;
import com.orhundalabasmaz.storm.utils.FileService;
import com.orhundalabasmaz.storm.utils.Logger;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TestStepDefinitions {
	private String testId;
	private ConfigurationBuilder configBuilder;
	private TestLoadBalancerTopology topology = new TestLoadBalancerTopology();

	@Before
	public void initialize() {
		Logger.log("test initializing...");
		initializeOutput();
	}

	private void initializeOutput() {
		// mkdir resources/output if not exists
		FileService.createDirectory("output");
		// rm file results.csv if exists
		FileService.deleteFile("output/results.csv");
	}

	@Given("^Data type and process duration$")
	public void dataTypeAndProcessDuration(List<RuntimeConfig> runtimeConfigList) throws Throwable {
		testId = DKGUtils.generateTestId();
		configBuilder = new ConfigurationBuilder();
		configBuilder.defaultSet();
		RuntimeConfig config = runtimeConfigList.get(0);
		Logger.log("dataType is: " + config.getDataType() + ", process duration is: " + config.getProcessDuration());
		configBuilder
				.testId(testId)
				.enableLogging(true)
				.processDuration(config.getProcessDuration())
				.terminationTimeout(config.getTerminationTimeout())
				.topologyTimeout(config.getTerminationTimeout() + 5_000);
	}

	@When("^Grouping type is (\\w+)$")
	public void groupingType(GroupingType groupingType) throws Throwable {
		Logger.log("methodType is: " + groupingType);
		configBuilder.groupingType(groupingType);
	}

	@And("^Stream type is (\\w+)$")
	public void streamType(StreamType streamType) throws Throwable {
		Logger.log("streamType is: " + streamType);
		configBuilder.streamType(streamType);
	}

	@And("^Spout count is (\\d+)$")
	public void spoutCount(Integer spoutCount) throws Throwable {
		Logger.log("spoutCount is: " + spoutCount);
		configBuilder.numberOfSpouts(spoutCount);
	}

	@And("^Worker count is (\\d+)$")
	public void workerCount(Integer workerCount) throws Throwable {
		Logger.log("workerCount is: " + workerCount);
		configBuilder.numberOfWorkerBolts(workerCount);
	}

	@Then("^Execute test$")
	public void executeTest(List<RuntimeConfig> runtimeConfigList) throws Throwable {
		Logger.log("test begins... " + DKGUtils.getCurrentDatetime());
		int retryCount = runtimeConfigList.get(0).getRetryCount();
		Configuration config = configBuilder.build();
		for (int i = 1; i <= retryCount; ++i) {
			Logger.log("test #" + i);
//			DKGUtils.sleepInSeconds(3);
			topology.testOnLocal(config);
		}
		Logger.log("test ends... " + DKGUtils.getCurrentDatetime());
	}

	@And("^Test successfully completed$")
	public void testSuccessfullyCompleted() throws Throwable {
		Logger.log("successfully completed.");
		//todo write output result to file

	}

	@After
	public void cleanUp() {
		Logger.log("test done.");
	}
}
