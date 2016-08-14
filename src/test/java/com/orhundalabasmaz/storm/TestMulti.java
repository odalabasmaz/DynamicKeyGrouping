package com.orhundalabasmaz.storm;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Orhun Dalabasmaz
 */
public class TestMulti {
	private TestLoadBalancerTopology DKP = new TestLoadBalancerTopology();

	@BeforeClass
	public static void beforeTest() {
		System.out.println("TestMulti: begin");
	}

	@Test
	public void testConf1() {
		System.out.println("begin conf: 1");
		DKP.testOnLocal();
		System.out.println("end conf: 1");
	}

	@Test
	public void testConf2() {
		System.out.println("begin conf: 2");
		DKP.testOnLocal();
		System.out.println("end conf: 2");
	}

	@Test
	public void testConf3() {
		System.out.println("begin conf: 3");
		DKP.testOnLocal();
		System.out.println("end conf: 3");
	}

	@AfterClass
	public static void afterTest() {
		System.out.println("TestMulti: end");
	}
}
