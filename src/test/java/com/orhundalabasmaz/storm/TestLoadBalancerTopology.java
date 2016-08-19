package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.common.ITopology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.loadBalancer.LoadBalancerTopology;
import org.junit.Test;

/**
 * @author Orhun Dalabasmaz
 */
public class TestLoadBalancerTopology {

	@Test
	public static void testOnLocal(Configuration runtimeConf) {
		ITopology topology = new LoadBalancerTopology(StormMode.LOCAL, runtimeConf);
		topology.init();
		topology.run();
	}

	@Test
	public static void testOnCluster(Configuration runtimeConf) {
		ITopology topology = new LoadBalancerTopology(StormMode.CLUSTER, runtimeConf);
		topology.init();
		topology.run();
	}
}