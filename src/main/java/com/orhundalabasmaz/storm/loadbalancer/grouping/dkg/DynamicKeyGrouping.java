package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamicKeyGrouping.class);
	private static final long serialVersionUID = 398118264736370294L;

	private long startingTime = 0L;
	private long warmUpDuration = 5_000L;     // ms (default: 30 sec)
	private int numberOfInitialTasks = 2;
	private int numberOfAvailableTasks;

	private long numOfItems = 0;
	private long[] targetTaskStats;
	private List<Integer> targetTasks;

	private double loadToScaleUp;
	private double loadToScaleDown;

	private final KeySpace keySpace;
	private final Map<String, Integer> workerCountSizeMap;

	public DynamicKeyGrouping() {
		String id = this.toString();
		LOGGER.info("DKG-D: {}", id);
		keySpace = new KeySpace();
		workerCountSizeMap = new HashMap<>();
	}

	public DynamicKeyGrouping(int distinctKeyCounts) {
		String id = this.toString();
		LOGGER.info("DKG: {}", id);
		keySpace = new KeySpace(distinctKeyCounts);
		workerCountSizeMap = new HashMap<>();
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId streamId, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		this.targetTaskStats = new long[targetTasks.size()];
		this.numberOfAvailableTasks = targetTasks.size();
		initThresholds();
		initStartingTime();
		initKeySpaceManagement();
	}

	private void initThresholds() {
		double idealLoad = (double) 100 / numberOfAvailableTasks;
		loadToScaleUp = idealLoad + Math.sqrt(idealLoad);
		loadToScaleDown = idealLoad - Math.sqrt(idealLoad);
		LOGGER.info("LoadToScaleUp: {} and LoadToScaleDown: {}", loadToScaleUp, loadToScaleDown);
	}

	private void initStartingTime() {
		if (startingTime == 0L) {
			startingTime = System.currentTimeMillis();
		}
	}

	private void initKeySpaceManagement() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(new KeySpaceManager(keySpace));
//		executor.submit(new KeySpaceGC(keySpace));
	}

	private boolean isWarmUp() {
		long now = System.currentTimeMillis();
		long timePassed = now - startingTime;
		return timePassed > warmUpDuration;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> chosenTasks = new ArrayList<>(1);
		if (!values.isEmpty()) {
			String key = values.get(0).toString();
			synchronized (keySpace) {
				Integer chosen = chooseBestTask(key);       // index of best task
				chosenTasks.add(targetTasks.get(chosen));
				handleKey(key, chosen);
			}
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
			workerCount = numberOfInitialTasks;      //i.e. 2 tasks available in startup
		}

		int numberOfLessLoad = 0;
		// select least loaded task as the best
		Integer bestTaskIndex = null;
		double minLoad = Double.MAX_VALUE;
		for (int i = 0; i < workerCount; ++i) {
			int taskIndex = normalizeIndex((long) targetWorkerIndex + i);
			double load = getCurrentLoadOfTask(taskIndex);
			if (load < loadToScaleUp) {
				numberOfLessLoad++;
			}
			if (load < minLoad) {
				minLoad = load;
				bestTaskIndex = taskIndex;
			}
		}

		if (shouldScaleUp(key, minLoad)) {
			// check if it should scale up
			int newTaskIndex = normalizeIndex((long) targetWorkerIndex + workerCount);
			double newTaskLoad = getCurrentLoadOfTask(newTaskIndex);
			if (newTaskLoad < minLoad) {
				bestTaskIndex = newTaskIndex;
				if (newTaskIndex >= numberOfInitialTasks) {
					workerCountSizeMap.put(key, workerCount + 1);
					LOGGER.info("Scaling up for key: {}, to: {} workers at dkg: {}", key, workerCount + 1, getDKGId());
				}
			}
		} else if (shouldScaleDown(workerCount, numberOfLessLoad)) {
			// check if it should scale down
			workerCountSizeMap.put(key, workerCount - 1);
			LOGGER.info("Scaling down for key: {}, to: {} workers at dkg: {}", key, workerCount - 1, getDKGId());
			return chooseBestTask(key);
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

	private boolean shouldScaleDown(int workerCount, int numberOfLessLoad) {
		return workerCount > 2 && numberOfLessLoad >= 2;
	}

	/**
	 * calculates the local load of the task
	 */
	private double getCurrentLoadOfTask(int task) {
		return numOfItems == 0 ? 0 : ((double) targetTaskStats[task] / numOfItems) * 100;
	}

	private int normalizeIndex(long index) {
		return (int) (index % numberOfAvailableTasks);
	}

	private String getDKGId() {
		return this.toString().split("@")[1].toUpperCase();
	}
}
