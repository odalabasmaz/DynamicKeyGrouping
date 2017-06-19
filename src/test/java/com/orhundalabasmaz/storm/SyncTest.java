package com.orhundalabasmaz.storm;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.orhundalabasmaz.storm.utils.Statistics;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Orhun Dalabasmaz
 */
public class SyncTest {
	private static final Random random = new Random();

	@Test
	public void sync() {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (int i = 0; i < 1_000_000_000; ++i) {
			calcSync();
		}
		stopWatch.stop();
		long duration = stopWatch.getTime();
		System.out.println(duration + " ms");
	}

	// runtime duration: 54547 ms
	private synchronized void calcSync() {
		int a = random.nextInt();
		int b = random.nextInt();
		int c = random.nextInt();
		int r = a * b + c;
	}

	// runtime duration: 40370 ms
	private void calcAsync() {
		int a = random.nextInt();
		int b = random.nextInt();
		int c = random.nextInt();
		int r = a * b + c;
	}

	@Test
	public void stdDev() {
		double[] values = new double[]{100, 0, 0, 0, 0, 0, 0, 0, 0, 0};    //30
//		double[] values = new double[]{100};
//		double[] values = new double[]{50,50,0,0,0,0,0,0,0,0};	//20
		double res = Statistics.standardDeviation(values);
		System.out.println(res);
	}

	@Test
	public void pkg() {
		HashFunction h1 = Hashing.murmur3_128(13);
		HashFunction h2 = Hashing.murmur3_128(17);

		int size = 10;
		String str = "Turkey";
		int firstChoice = (int) (Math.abs(h1.hashBytes(str.getBytes()).asLong()) % size);
		int secondChoice = (int) (Math.abs(h2.hashBytes(str.getBytes()).asLong()) % size);

		System.out.printf("f1: %s, f2: %s\n", firstChoice, secondChoice);
	}

	@Test
	public void xxx() {
		Map<String, Long> map = new HashMap<>(10);
		double[] metrics;
		int size = 10;
		System.out.println("size: " + size);
		metrics = new double[size];
		metrics[0] = 100;
		metrics[1] = 0;
		System.out.println(Statistics.standardDeviation(metrics));
	}
}
