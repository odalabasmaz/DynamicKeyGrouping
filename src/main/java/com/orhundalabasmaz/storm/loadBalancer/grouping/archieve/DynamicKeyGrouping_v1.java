package com.orhundalabasmaz.storm.loadBalancer.grouping.archieve;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.orhundalabasmaz.storm.loadBalancer.Configuration;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class DynamicKeyGrouping_v1 { //implements CustomStreamGrouping, Serializable {
	/*private static final long serialVersionUID = -6900707578779084100L;

	private final int NUMBER_OF_INITIAL_TASKS = 2;
	private final int NUMBER_OF_MAX_TASKS = Configuration.N_COUNTER_BOLTS;
	private final int NUMBER_OF_MAX_RETRY = 10;
	private final int THRESHOLD_NUMBER_OF_ITEMS = 1000;
	private final int THRESHOLD_LOAD_PERCENTAGE = 100 / (NUMBER_OF_MAX_TASKS - 1);
	private final int THRESHOLD_HIGH_LOAD_PERCENTAGE = 40;
	private final int THRESHOLD_MIN_LOAD_PERCENTAGE = 5;

	private List<Integer> targetTasks;
	private long[] targetTaskStats;
	private long numOfItems = 0;

	private final List<Integer> primeNumbers = new ArrayList<>();
	private final Map<Integer, HashFunction> hashFunctionMap = new HashMap<>();
	private final Map<String, List<Integer>> keyTargetMap = new HashMap<>();
	private final Map<String, Integer> keyCountMap = new HashMap<>();

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId streamId, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		this.targetTaskStats = new long[targetTasks.size()];
		this.initializePrimeNumbers();
		this.initializeHashFunctionMap();
	}

	private void initializePrimeNumbers() {
		primeNumbers.add(13);
		primeNumbers.add(17);
		primeNumbers.add(19);
		primeNumbers.add(23);
		primeNumbers.add(29);
	}

	private void initializeHashFunctionMap() {
		//todo: we should not use this function, getNextHashSeed() method breaks it because we have not a formula for prime numbers
		for (int i = 0; i < primeNumbers.size(); ++i) {
			hashFunctionMap.put(i, Hashing.murmur3_128(primeNumbers.get(i)));
		}
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> chosenTasks = new ArrayList<>(1);
		if (!values.isEmpty()) {
			String key = values.get(0).toString();
			int chosen = chooseBestTask(key);
			chosenTasks.add(targetTasks.get(chosen));
			handleKey(key, chosen);
		}
		return chosenTasks;
	}

	private void handleKey(String key, int chosen) {
		targetTaskStats[chosen]++;
		numOfItems++;
		Integer count = keyCountMap.get(key);
		if (count == null) {
			count = 0;
		}
		keyCountMap.put(key, ++count);
	}

	private Integer chooseBestTask(String key) {
		// check if no task is assigned yet
		List<Integer> targetList = keyTargetMap.get(key);
		if (targetList == null) {
			targetList = new ArrayList<>();
			keyTargetMap.put(key, targetList);
			*//*if (NUMBER_OF_INITIAL_TASKS < 1
					|| NUMBER_OF_INITIAL_TASKS > NUMBER_OF_MAX_TASKS) {
				throw new UnsupportedOperationException("check number of initial tasks and number of max tasks");
			}*//*
			Integer targetTask = getNewTargetTask(targetList, key);
			*//*if (targetTask == null) {
				throw new UnsupportedOperationException("targetTask can not be null, any bolt ?");
			}*//*
			for (int i = 1; i < NUMBER_OF_INITIAL_TASKS; ++i) {
				getNewTargetTask(targetList, key);
			}
			return targetTask;
		}

		int bestTask = targetList.get(0);
		double minLoad = Double.MAX_VALUE;
		for (Integer task : targetList) {
			double load = getCurrentLoadOfTask(task);
			if (load < minLoad) {
				minLoad = load;
				bestTask = task;
			}
		}

		//todo >> consider key-based load before distribution
		if (shouldChooseNewTarget(key, targetList, minLoad)) {
			Integer targetTask = getNewTargetTask(targetList, key);
			if (targetTask != null
					&& getCurrentLoadOfTask(targetTask) < minLoad) {
				bestTask = targetTask;
			}
		}

		return bestTask;
	}

	private boolean shouldChooseNewTarget(String key, List<Integer> targetList, double minLoad) {
		return numOfItems > THRESHOLD_NUMBER_OF_ITEMS
				&& minLoad > THRESHOLD_LOAD_PERCENTAGE
				&& targetList.size() < NUMBER_OF_MAX_TASKS
				&& (
				keyCountMap.get(key) != null && keyCountMap.get(key) > THRESHOLD_NUMBER_OF_ITEMS
						&& (minLoad > THRESHOLD_HIGH_LOAD_PERCENTAGE || getCurrentLoadOfKey(key) > THRESHOLD_MIN_LOAD_PERCENTAGE)
		);
	}

	private Integer getNewTargetTask(List<Integer> targetList, String key) {
		for (int i = 0; i < NUMBER_OF_MAX_RETRY; ++i) {
			int targetTask = getTargetTask(i, key);
			if (!targetList.contains(targetTask)) {
				targetList.add(targetTask);
				return targetTask;
			}
		}
		return null;
	}

	private int getTargetTask(int h, String key) {
		return getTargetTask(getHashFunction(h), key);
	}

	private int getTargetTask(HashFunction hf, String key) {
		return (int) (Math.abs(hf.hashBytes(key.getBytes()).asLong()) % this.targetTasks.size());
	}

	private double getCurrentLoadOfTask(int task) {
		return ((double) targetTaskStats[task] / numOfItems) * 100;
	}

	private double getCurrentLoadOfKey(String key) {
		return ((double) keyCountMap.get(key) / numOfItems) * 100;
	}

	private HashFunction getHashFunction(int h) {
		HashFunction hf = hashFunctionMap.get(h);
		if (hf == null) {
			int hn = getNextHashSeed(h);
			hf = Hashing.murmur3_128(hn);
			hashFunctionMap.put(h, hf);
		}
		return hf;
	}

	private int getNextHashSeed(int h) {
		//todo: should be prime ?
		int seed = primeNumbers.get(h - 1);
		do {
			seed += 2;
		} while (!isPrime(seed));
		primeNumbers.add(seed);
		return seed;
	}

	private boolean isPrime(int num) {
		if (num < 2) return false;
		if (num == 2) return true;
		if (num % 2 == 0) return false;
		double sqrt = Math.sqrt(num);
		for (int i = 3; i <= sqrt; i += 2) {
			if (num % i == 0) return false;
		}
		return true;
	}*/
}
