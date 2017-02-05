package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.orhundalabasmaz.storm.utils.DKGUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Orhun Dalabasmaz
 */
public class DynamicKeyGrouping implements CustomStreamGrouping, Serializable {
	private static final long serialVersionUID = 398118264736370294L;

	private long STARTING_TIME = 0L;
	private long WARM_UP_DURATION = 5_000L;     // ms (default: 30 sec)
	private int NUMBER_OF_INITIAL_TASKS = 2;
	private int NUMBER_OF_AVAILABLE_TASKS;

	private long numOfItems = 0;
	private long[] targetTaskStats;
	private List<Integer> targetTasks;

	private double loadToScaleUp;
	private double loadToScaleDown;

	private final KeySpace keySpace;
	private final Map<String, Integer> workerCountSizeMap;

	public DynamicKeyGrouping() {
		keySpace = new KeySpace();
		workerCountSizeMap = new HashMap<>();
	}

	public DynamicKeyGrouping(int distinctKeyCounts) {
		keySpace = new KeySpace(distinctKeyCounts);
		workerCountSizeMap = new HashMap<>();
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId streamId, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		this.targetTaskStats = new long[targetTasks.size()];
		this.NUMBER_OF_AVAILABLE_TASKS = targetTasks.size();
		initThresholds();
		initStartingTime();
		initKeySpaceManagement();
	}

	private void initThresholds() {
		double idealLoad = (double) 100 / NUMBER_OF_AVAILABLE_TASKS;
		loadToScaleUp = idealLoad + Math.sqrt(idealLoad);
		loadToScaleDown = idealLoad - Math.sqrt(idealLoad);
	}

	private void initStartingTime() {
		if (STARTING_TIME == 0L) {
			STARTING_TIME = System.currentTimeMillis();
		}
	}

	private void initKeySpaceManagement() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(new KeySpaceManager(keySpace));
		executor.submit(new KeySpaceGC(keySpace));
	}

	private boolean isWarmUp() {
		long now = System.currentTimeMillis();
		long timePassed = now - STARTING_TIME;
		return timePassed > WARM_UP_DURATION;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> chosenTasks = new ArrayList<>(1);
		if (!values.isEmpty()) {
			String key = values.get(0).toString();
			Integer chosen = chooseBestTask(key);       // index of best task
			chosenTasks.add(targetTasks.get(chosen));
			handleKey(key, chosen);
		}
		return chosenTasks;
	}

	private void handleKey(String key, int chosen) {
		targetTaskStats[chosen]++;
		numOfItems++;
		keySpace.handleKey(key);
	}

	private Integer chooseBestTask(String key) {
		long hashOfKey = DKGUtils.calculateHash(key);
		int targetWorkerIndex = normalizeIndex(hashOfKey);
		Integer workerCount = workerCountSizeMap.get(key);
		if (workerCount == null) {
			workerCount = NUMBER_OF_INITIAL_TASKS;      //i.e. 2 tasks available in startup
		}

		// select least loaded task as the best
		Integer bestTaskIndex = null;
		double minLoad = Double.MAX_VALUE;
		for (int i = 0; i < workerCount; ++i) {
			int taskIndex = normalizeIndex(targetWorkerIndex + i);
			double load = getCurrentLoadOfTask(taskIndex);
			if (load < minLoad) {
				minLoad = load;
				bestTaskIndex = taskIndex;
			}
		}

		// check if it should scale up
		boolean scaleUp = shouldScaleUp(key, minLoad);
		if (scaleUp) {
			int newTaskIndex = normalizeIndex(targetWorkerIndex + workerCount);
			double newTaskLoad = getCurrentLoadOfTask(newTaskIndex);
			if (newTaskLoad < minLoad) {
				bestTaskIndex = newTaskIndex;
				if (newTaskIndex >= NUMBER_OF_INITIAL_TASKS) {
					workerCountSizeMap.put(key, workerCount + 1);
				}
			}
		}

		return bestTaskIndex;
	}

	private boolean shouldScaleUp(String key, double minLoad) {
		// is warm up
		if (!isWarmUp()) {
			return false;
		}

		// check threshold
		if (minLoad < loadToScaleUp) {
			return false;
		}

		// is in old-gen
		if (!keySpace.inOldSpace(key)) {
			return false;
		}

		return true;
	}

	/**
	 * calculates the local load of the task
	 */
	private double getCurrentLoadOfTask(int task) {
		return numOfItems == 0 ? 0 : ((double) targetTaskStats[task] / numOfItems) * 100;
	}

	private int normalizeIndex(long index) {
		return (int) (index % NUMBER_OF_AVAILABLE_TASKS);
	}

}
