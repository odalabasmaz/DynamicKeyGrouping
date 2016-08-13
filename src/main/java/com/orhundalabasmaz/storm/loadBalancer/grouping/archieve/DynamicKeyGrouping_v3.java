package com.orhundalabasmaz.storm.loadBalancer.grouping.archieve;

import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.KeySpace;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.KeySpaceGC;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.KeySpaceManager;
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
public class DynamicKeyGrouping_v3 implements CustomStreamGrouping, Serializable {
	private static final long serialVersionUID = 398118264736370294L;

	private long STARTING_TIME = 0L;
	private long WARM_UP_DURATION = 5_000L;     // ms (default: 30 sec)
	private int NUMBER_OF_INITIAL_TASKS;
	private int NUMBER_OF_AVAILABLE_TASKS;

	private long numOfItems = 0;
	private long[] targetTaskStats;
	private List<Integer> targetTasks;

	private double idealLoad;
	private double loadToScaleUp;
	private double loadToScaleDown;

	private final KeySpace keySpace = new KeySpace();
	private final Map<String, Integer> keyHashMap = new HashMap<>();
	private final List<List<Integer>> hashTaskList = new ArrayList<>();

	public DynamicKeyGrouping_v3() {
		System.out.println("DynamicKeyGrouping constructor");
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId streamId, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		this.targetTaskStats = new long[targetTasks.size()];
		this.NUMBER_OF_INITIAL_TASKS = 2;
		this.NUMBER_OF_AVAILABLE_TASKS = targetTasks.size();
		initThresholds();
		initHashTaskList();
		initStartingTime();
		initKeySpaceManagement();
	}

	private void initThresholds() {
		idealLoad = (double) 100 / NUMBER_OF_AVAILABLE_TASKS;
		loadToScaleUp = idealLoad + Math.sqrt(idealLoad);
		loadToScaleDown = idealLoad - Math.sqrt(idealLoad);
	}

	private void initHashTaskList() {
		DKGUtils.arrangeHashTaskList(hashTaskList, NUMBER_OF_AVAILABLE_TASKS);
	}

	private void initStartingTime() {
		if (STARTING_TIME == 0L) {
			STARTING_TIME = System.currentTimeMillis();
		}
	}

	private void initKeySpaceManagement() {
		new Thread(new KeySpaceManager(keySpace)).start();
		new Thread(new KeySpaceGC(keySpace)).start();

//		ExecutorService executor = Executors.newSingleThreadExecutor();
//		executor.submit(new KeySpaceManager(keySpace));
//		executor.submit(new KeySpaceGC(keySpace));
	}

	private boolean isWarmUp() {
		long now = System.currentTimeMillis();
		long timePassed = now - STARTING_TIME;
//		System.out.println("## time passed: " + timePassed + " ms");
		return timePassed > WARM_UP_DURATION;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> chosenTasks = new ArrayList<>(1);
		if (!values.isEmpty()) {
			String key = values.get(0).toString();
			Integer chosen = chooseBestTask(key);
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
		int taskListIndex = (int) (hashOfKey % NUMBER_OF_AVAILABLE_TASKS);
		Integer lastIndex = keyHashMap.get(key);
		if (lastIndex == null) {
			lastIndex = NUMBER_OF_INITIAL_TASKS - 1;      //i.e. 2 tasks available in startup
		}

		// select least loaded task as the best
		List<Integer> tasks = hashTaskList.get(taskListIndex);
		Integer bestTask = null;
		double minLoad = Double.MAX_VALUE;
		for (int taskIndex = 0; taskIndex <= lastIndex; ++taskIndex) {
			int task = tasks.get(taskIndex);
			double load = getCurrentLoadOfTask(task);
			if (load < minLoad) {
				minLoad = load;
				bestTask = task;
			}
		}

		// check if it should scale up
		boolean scaleUp = shouldScaleUp(key, minLoad);
		if (scaleUp) {
			int newTaskIndex = lastIndex + 1;
			int newTask = tasks.get(newTaskIndex);
			double newTaskLoad = getCurrentLoadOfTask(newTask);
			if (newTaskLoad < minLoad) {
				bestTask = newTask;
				if (newTaskIndex >= NUMBER_OF_INITIAL_TASKS) {
					keyHashMap.put(key, newTaskIndex);
				}
			}
		}

		return bestTask;
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
		if (numOfItems == 0) return 0;
		return ((double) targetTaskStats[task] / numOfItems) * 100;
	}

	/*private double getCurrentLoadOfKey(String key) {
		return ((double) keyCountMap.get(key) / numOfItems) * 100;
	}*/

}
