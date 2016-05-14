package com.orhundalabasmaz.storm.loadBalancer.grouping;

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
public class DynamicKeyGrouping implements CustomStreamGrouping, Serializable {
	private static final long serialVersionUID = -6900707578779084100L;

	private static final long THRESHOLD_NUMBER_OF_ITEMS = 1000;
	private static final long THRESHOLD_LOAD_PERCENTAGE = 25;  // %40
	private static final int NUMBER_OF_INITIAL_TASKS = 2;
	private static final int NUMBER_OF_MAX_TASKS = Configuration.N_COUNTER_BOLTS;
	private static final int NUMBER_OF_MAX_RETRY = 10;

	private List<Integer> targetTasks;
	private long[] targetTaskStats;
	private long numOfItems = 0;

	private final List<Integer> primeNumbers = new ArrayList<>();
	private final Map<Integer, HashFunction> hashFunctionMap = new HashMap<>();
	private final Map<String, List<Integer>> keyTargetMap = new HashMap<>();

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
			targetTaskStats[chosen]++;
			numOfItems++;
		}
		return chosenTasks;
	}

	private int chooseBestTaskOrj(String key) {
		//todo: should choose least loaded bolt if there are multi candidate
		int h = 0;
		HashFunction hf = getHashFunction(h);
		int firstChoose = getTargetTask(hf, key);
		if (numOfItems < THRESHOLD_NUMBER_OF_ITEMS) {
			// do nothing, best is chosen already..
			return firstChoose;
		}

		int chosen = firstChoose;
		while (true) {
			if (getCurrentLoadOfTask(chosen) < THRESHOLD_LOAD_PERCENTAGE) {
				break;
			}

			hf = getHashFunction(++h);
			chosen = getTargetTask(hf, key);

			if (h == NUMBER_OF_MAX_RETRY) {
				break;
			}
		}
		return chosen;
	}

	private Integer chooseBestTask(String key) {
		// check if no task is assigned yet
		List<Integer> targetList = keyTargetMap.get(key);
		if (targetList == null || targetList.isEmpty()) {
			targetList = new ArrayList<>();
			keyTargetMap.put(key, targetList);
			/*if (NUMBER_OF_INITIAL_TASKS < 1
					|| NUMBER_OF_INITIAL_TASKS > NUMBER_OF_MAX_TASKS) {
				throw new UnsupportedOperationException("check number of initial tasks and number of max tasks");
			}*/
			Integer targetTask = getNewTargetTask(targetList, key);
			/*if (targetTask == null) {
				throw new UnsupportedOperationException("targetTask can not be null, any bolt ?");
			}*/
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
//		int bestTask = getTargetTask(bestTargetTask, key);
		if (numOfItems > THRESHOLD_NUMBER_OF_ITEMS
				&&
				minLoad > THRESHOLD_LOAD_PERCENTAGE
				&&
				targetList.size() < NUMBER_OF_MAX_TASKS) {
			//todo: get another bolt and return if available
			Integer targetTask = getNewTargetTask(targetList, key);
			if (targetTask != null) {
				bestTask = targetTask;
			}
		}

		return bestTask;
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

	private HashFunction getHashFunction(int h) {
//		if (h == NUMBER_OF_MAX_TASKS) {
//		if (h == NUMBER_OF_MAX_RETRY) {
		//todo: should return least loaded one
//			return hashFunctionMap.get(h);
//			return null;
//		}

		//todo: should control if hash function returns the same key as before xx
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

	private int getNextHashSeedOld(int h) {
		//todo: should be prime ?
		int seed;
		do {
			seed = 13 + 2 * h;
			h += 2;
		}
		while (!isPrime(seed));
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
	}
}
