package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.config.ConfigurationBuilder;
import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.StreamType;
import org.junit.*;

/**
 * @author Orhun Dalabasmaz
 */
public class TestMulti {
	private TestLoadBalancerTopology DKP = new TestLoadBalancerTopology();
	private Configuration runtimeConf;
	private int testCase = 0;
	private boolean logEnabled = true;

	@BeforeClass
	public static void beforeClass() {
		System.out.println("TestMulti: begin");
		//todo: read all input from excel
		//todo: init output file
	}

	@AfterClass
	public static void afterClass() {
		System.out.println("TestMulti: end");
	}

	@Before
	public void before() {
		runtimeConf = new ConfigurationBuilder()
				.numberOfWorkers(1)
				.numberOfSpouts(1)
				.numberOfSplitterBolts(3)
				.numberOfWorkerBolts(50)
				.numberOfAggregatorBolts(1)
				.numberOfResultBolts(1)
				.numberOfTasks(2)
				.timeIntervalOfDataStreams(1)
				.timeIntervalOfWorkerBolts(5)
				.timeIntervalOfAggregatorBolts(15)
				.terminationTimeout(1 * 60 * 1000)
				.topologyTimeout(1 * 60 * 1000 + 10_000)
				.streamType(StreamType.HOMOGENEOUS)
				.groupingType(GroupingType.DYNAMIC_KEY)
				.aggregatorType(AggregatorType.CUMULATIVE)
				.processDuration(10)
				.aggregationDuration(0)
				.enableLogging(logEnabled)
				.build();
	}

	@After
	public void after() {
		runtimeConf = null;
		testCase++;
	}

	@Test
	public void loop() {
		System.out.println("loop");
	}

	@Test
	public void testConf1() {
		System.out.println("begin runtimeConf: 1");
		DKP.testOnLocal(runtimeConf);
		System.out.println("end runtimeConf: 1");
	}

	@Test
	public void testConf2() {
		System.out.println("begin runtimeConf: 2");
		DKP.testOnLocal(runtimeConf);
		System.out.println("end runtimeConf: 2");
	}

	@Test
	public void testConf3() {
		System.out.println("begin runtimeConf: 3");
		DKP.testOnLocal(runtimeConf);
		System.out.println("end runtimeConf: 3");
	}

}
