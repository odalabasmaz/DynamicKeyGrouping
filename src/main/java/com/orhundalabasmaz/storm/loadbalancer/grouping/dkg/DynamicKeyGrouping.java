package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.orhundalabasmaz.storm.common.ObjectObserver;
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
public class DynamicKeyGrouping implements CustomStreamGrouping, ObjectObserver, Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamicKeyGrouping.class);
	private static final long serialVersionUID = 398118264736370294L;

	private long startingTime;
	private long warmUpDuration = 15_000L;     // ms (default: 30 sec)
	private int numberOfInitialTasks = 2;
	private int numberOfAvailableTasks;
	private long checkInterval = 60 * 1000L;

	private long numOfItems;
	private long[] targetTaskStats;
	private List<Integer> targetTasks;

	private int loadToScaleUp;
	private int loadToScaleDown;

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
		loadToScaleUp = (int) (idealLoad + Math.sqrt(idealLoad));
		loadToScaleDown = (int) (idealLoad - Math.sqrt(idealLoad));
		LOGGER.info("LoadToScaleUp: {} and LoadToScaleDown: {}", loadToScaleUp, loadToScaleDown);
	}

	private void initStartingTime() {
		if (startingTime == 0L) {
			startingTime = System.currentTimeMillis();
		}
	}

	private void initKeySpaceManagement() {
		LOGGER.info("KeySpaceManagement is being initialized...");
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
				Integer targetTask = targetTasks.get(chosen);
				chosenTasks.add(targetTask);
				handleKey(key, chosen, targetTask);
			}
		}
		return chosenTasks;
	}

	private void handleKey(String key, Integer chosen, Integer targetTask) {
		targetTaskStats[chosen]++;
		numOfItems++;
		keySpace.handleKey(key, targetTask);
	}

	private Integer chooseBestTask(String key) {
		long hashOfKey = DKGUtils.calculateHash(key);
		int targetWorkerIndex = normalizeIndex(hashOfKey);
		Integer workerCount = workerCountSizeMap.get(key);
		if (workerCount == null) {
			workerCount = numberOfInitialTasks;      //i.e. 2 tasks available in startup
		}

		// workers which has less load
		int numberOfLessLoad = 0;

		// select least loaded task as the best
		Integer bestTaskIndex = null;
		int minLoad = Integer.MAX_VALUE;
		for (int i = 0; i < workerCount; ++i) {
			int taskIndex = normalizeIndex((long) targetWorkerIndex + i);
			int load = getCurrentLoadOfTask(taskIndex);
			if (load < loadToScaleUp) {
				numberOfLessLoad++;
			}
			if (load < minLoad) {
				minLoad = load;
				bestTaskIndex = taskIndex;
			}
		}

		// wait for a while before scaling
		if (canCheckForScaling(key)) {
			LOGGER.info("Checking for scaling...");
			if (shouldScaleUp(key, minLoad)) {
				// check if it should scale up
				int newTaskIndex = normalizeIndex((long) targetWorkerIndex + workerCount);
				int newTaskLoad = getCurrentLoadOfTask(newTaskIndex);
				if (newTaskLoad < minLoad) {
					bestTaskIndex = newTaskIndex;
					workerCountSizeMap.put(key, workerCount + 1);
//					LOGGER.info("#WS: workerCountSizeMap.size() = {}", workerCountSizeMap.size());
					LOGGER.info("Scaling up for key: {}, to: {} workers at dkg: {} with newTaskLoad/minLoad: {}/{}",
							key, workerCount + 1, getObjectId(), newTaskLoad, minLoad);
				}
			} else if (shouldScaleDown(workerCount, numberOfLessLoad)) {
				// check if it should scale down
				int newCount = workerCount - 1;
				if (newCount <= 2) workerCountSizeMap.remove(key);
				else workerCountSizeMap.put(key, newCount);
				LOGGER.info("Scaling down for key: {}, to: {} workers at dkg: {} for load: {}",
						key, newCount, getObjectId(), minLoad);
				return chooseBestTask(key);
			}
		}
		printKeyWorkerCount();
		return bestTaskIndex;
	}

	private long lastTimestamp = 0L;

	private void printKeyWorkerCount() {
		long now = DKGUtils.getCurrentTimestamp();
		if (now - lastTimestamp > 30_000) {
			lastTimestamp = now;
			Integer count = workerCountSizeMap.get("Turkey");
			LOGGER.info("#TC: {},{}", now, count);
		}
	}

	private boolean canCheckForScaling(String key) {
		KeyItem keyItem = keySpace.findKeyItem(key);
		if (keyItem == null) {
			return false;
		}

		long now = DKGUtils.getCurrentTimestamp();
		long latestCheckTime = keyItem.getLastCheckTimeForScaling();
		if (now - latestCheckTime > checkInterval) {
			keyItem.setLastCheckTimeForScaling(now);
			return true;
		}
		return false;
	}

	private boolean shouldScaleUp(String key, int minLoad) {
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
	private int getCurrentLoadOfTask(int task) {
		return numOfItems == 0 ? 0 : (int) ((100 * targetTaskStats[task]) / numOfItems);
	}

	private int normalizeIndex(long index) {
		return (int) (index % numberOfAvailableTasks);
	}
}
