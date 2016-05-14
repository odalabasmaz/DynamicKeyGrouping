package com.orhundalabasmaz.storm;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
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

	@Test
	public void testHash() {
		String key = "turkey";
		int size = 4;
		int[] primes = {13, 17, 19, 23, 29, 31, 37, 41, 43, 47};
		for (int prime : primes) {
			HashFunction hf = Hashing.murmur3_128(prime);
			int hr = (int) (Math.abs(hf.hashBytes(key.getBytes()).asLong()) % size);
			System.out.println(prime + " > " + hr);
		}

	}
}