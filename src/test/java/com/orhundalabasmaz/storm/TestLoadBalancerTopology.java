package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.common.ITopology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.loadBalancer.LoadBalancerTopology;
import org.junit.Test;

/**
 * @author Orhun Dalabasmaz
 */
public class TestLoadBalancerTopology {

	@Test
	public void testOnLocal() {
		final ITopology topology = new LoadBalancerTopology(StormMode.LOCAL);
		topology.init();
		topology.run();
	}

	@Test
	public void testOnCluster() {
		final ITopology topology = new LoadBalancerTopology(StormMode.CLUSTER);
		topology.init();
		topology.run();
	}
}