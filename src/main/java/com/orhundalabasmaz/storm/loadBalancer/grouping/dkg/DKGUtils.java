package com.orhundalabasmaz.storm.loadBalancer.grouping.dkg;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Orhun Dalabasmaz
 */
public class DKGUtils {
	private static final HashFunction HF = Hashing.murmur3_128(13);

	public static long calculateHash(String key) {
		return Math.abs(HF.hashBytes(key.getBytes()).asLong());
	}

	public static void arrangeHashTaskList(List<List<Integer>> hashTaskList, int taskSize) {
		// 0 -> 1 2 3
		// 1 -> 1 3 2
		// ...
		List<Integer> domain = new ArrayList<>(taskSize);
		for (int i = 0; i < taskSize; ++i) {
			domain.add(i);
		}
		calcPerms(hashTaskList, domain, new ArrayList<>(taskSize));
	}

	private static void calcPerms(List<List<Integer>> hashTaskList, List<Integer> domain, List<Integer> curr) {
		if (domain.isEmpty()) {
			hashTaskList.add(new ArrayList<>(curr));
			return;
		}
		for (int i = 0; i < domain.size(); ++i) {
			Integer in = domain.remove(i);
			curr.add(in);
			calcPerms(hashTaskList, domain, curr);
			curr.remove(in);
			domain.add(i, in);
		}
	}

	public static void sleepInMicroseconds(long duration) {
		try {
			TimeUnit.MICROSECONDS.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void sleepInMilliseconds(long duration) {
		try {
			TimeUnit.MILLISECONDS.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void sleepInSeconds(long duration) {
		try {
			TimeUnit.SECONDS.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
