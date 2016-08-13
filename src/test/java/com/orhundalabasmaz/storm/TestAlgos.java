package com.orhundalabasmaz.storm;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 */
public class TestAlgos {

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

	@Test
	public void calcPerm() {
		printPermResult(4);
		printPermResult(10);
		printPermResult(20);
		printPermResult(50);
		printPermResult(100);
	}

	private void printPermResult(long num) {
		System.out.println(num + "! " + perm(num));
	}

	private long perm(long num) {
		if (num < 0) return -1;
		if (num == 0) return 1;
		return num * perm(num - 1);
	}

	@Test
	public void testTaskPermutation() {
		String values = "1234567890";
		List<String> perms = new ArrayList<>();
		calcPerms(perms, values, "");
		for (String perm : perms) {
			System.out.println(perm);
		}
	}

	private void calcPerms(List<String> perms, String values, String curr) {
		if (values.length() == 0) {
//			perms.add(curr);
			System.out.println(curr);
			return;
		}
		for (int i = 0; i < values.length(); ++i) {
			String value = values.substring(i, i + 1);
			String remain = values.substring(0, i) + values.substring(i + 1);
			calcPerms(perms, remain, curr + value);
		}
	}

	@Test
	public void testListPermutation() {
		int taskSize = 10;
		List<List<Integer>> hashTaskList = new ArrayList<>(taskSize);
		arrangeHashTaskList(hashTaskList, taskSize);
		System.out.println(hashTaskList);
	}

	private void arrangeHashTaskList(List<List<Integer>> hashTaskList, int taskSize) {
		// 0 -> 1 2 3
		// 1 -> 1 3 2
		// ...
		List<Integer> domain = new ArrayList<>(taskSize);
		for (int i = 1; i <= taskSize; ++i) {
			domain.add(i);
		}
		calcPerms(hashTaskList, domain, new ArrayList<>(taskSize));
	}

	private void calcPerms(List<List<Integer>> hashTaskList, List<Integer> domain, List<Integer> curr) {
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
}
