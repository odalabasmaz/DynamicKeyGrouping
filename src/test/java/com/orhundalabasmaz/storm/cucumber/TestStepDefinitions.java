package com.orhundalabasmaz.storm.cucumber;

import com.orhundalabasmaz.storm.TestLoadBalancerTopology;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;
import com.orhundalabasmaz.storm.loadBalancer.spouts.StreamType;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TestStepDefinitions {
	private ConfigurationBuilder configBuilder;
	private TestLoadBalancerTopology topology = new TestLoadBalancerTopology();

	@Given("^Data type and process duration$")
	public void dataTypeAndProcessDuration(List<RuntimeConfig> runtimeConfigList) throws Throwable {
		configBuilder = new ConfigurationBuilder();
		configBuilder.defaultSet();
		RuntimeConfig config = runtimeConfigList.get(0);
		System.out.println("dataType is: " + config.getDataType() + ", process duration is: " + config.getProcessDuration() + "\n");
		configBuilder
				.processDuration(config.getProcessDuration())
				.terminationTimeout(config.getTerminationTimeout())
				.topologyTimeout(config.getTerminationTimeout() + 10_000);
	}

	@When("^Grouping type is (\\w+)$")
	public void groupingType(GroupingType groupingType) throws Throwable {
		System.out.println("methodType is: " + groupingType);
		configBuilder.groupingType(groupingType);
	}

	@And("^Stream type is (\\w+)$")
	public void streamType(StreamType streamType) throws Throwable {
		System.out.println("streamType is: " + streamType);
		configBuilder.streamType(streamType);
	}

	@And("^Spout count is (\\d+)$")
	public void spoutCount(Integer spoutCount) throws Throwable {
		System.out.println("spoutCount is: " + spoutCount);
		configBuilder.numberOfSpouts(spoutCount);
	}

	@And("^Worker count is (\\d+)$")
	public void workerCount(Integer workerCount) throws Throwable {
		System.out.println("workerCount is: " + workerCount);
		configBuilder.numberOfWorkerBolts(workerCount);
	}

	@Then("^Execute test$")
	public void executeTest() throws Throwable {
		System.out.println("test begins... " + DKGUtils.getCurrentDatetime());
		Configuration config = configBuilder.build();
		DKGUtils.sleepInSeconds(3);
//		topology.testOnLocal(config);
		System.out.println("test ends... " + DKGUtils.getCurrentDatetime());
	}

	@And("^Test successfully completed$")
	public void testSuccessfullyCompleted() throws Throwable {
		System.out.println("successfully completed.");
	}

}
