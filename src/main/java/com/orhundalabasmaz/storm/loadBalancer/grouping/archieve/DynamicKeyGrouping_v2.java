package com.orhundalabasmaz.storm.loadBalancer.grouping.archieve;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Orhun Dalabasmaz
 */
public class DynamicKeyGrouping_v2 implements CustomStreamGrouping, Serializable {
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
		Utils.arrangeHashTaskList(hashTaskList, NUMBER_OF_AVAILABLE_TASKS);
	}

	private void initStartingTime() {
		if (STARTING_TIME == 0L) {
			STARTING_TIME = System.currentTimeMillis();
		}
	}

	private void initKeySpaceManagement() {
		new Thread(new KeySpaceManager(keySpace)).start();
		new Thread(new KeySpaceGC(keySpace)).start();

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
		long hashOfKey = Utils.calculateHash(key);
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

	/**
	 * JVM-like architecture
	 * archived / eden-space / old-space / retired
	 */
	private class KeySpace implements Serializable {
		private final Map<String, Integer> keyCountMap = new HashMap<>();
		private final List<KeyItem> babySpace = new LinkedList<>();
		private final List<KeyItem> teenageSpace = new LinkedList<>();
		private final List<KeyItem> oldSpace = new LinkedList<>();

		private void handleKeyOld(String key) {
			Integer count = keyCountMap.get(key);
			if (count == null) {
				count = 0;
			}
			keyCountMap.put(key, ++count);
		}

		private void handleKey(String key) {
			KeyItem item = findKeyItem(key);
			if (item != null) {
				item.appearedAgain();
			} else {
				item = new KeyItem(key);
				babySpace.add(item);
			}
		}

		private KeyItem findKeyItem(String key) {
			KeyItem item;
			item = findKeyItemInList(key, oldSpace);
			if (item != null) return item;
			item = findKeyItemInList(key, teenageSpace);
			if (item != null) return item;
			item = findKeyItemInList(key, babySpace);
			if (item != null) return item;
			return null;
		}

		private KeyItem findKeyItemInList(String key, List<KeyItem> keyItemList) {
			for (KeyItem item : keyItemList) {
				if (item.key.equals(key)) {
					return item;
				}
			}
			return null;
		}

		private boolean inBabySpace(String key) {
			KeyItem item = findKeyItemInList(key, babySpace);
			return item != null;
		}

		private boolean inTeenageSpace(String key) {
			KeyItem item = findKeyItemInList(key, teenageSpace);
			return item != null;
		}

		private boolean inOldSpace(String key) {
			KeyItem item = findKeyItemInList(key, oldSpace);
			return item != null;
		}

		private void downToTeenageSpace(String key) {

		}

		private void upToTeenageSpace(String key) {

		}

		private void upToOldSpace(String key) {

		}

		private void archive(String key) {

		}

		private void retire(String key) {

		}

		private void emptyBabySpace() {
			babySpace.clear();
		}
	}

	private class KeyItem implements Comparable {
		private String key;
		private long lastSeen;
		private long count;

		public KeyItem(String key) {
			this.key = key;
			this.count = 0;
			this.lastSeen = System.currentTimeMillis();
		}

		public void appearedAgain() {
			++count;
			lastSeen = System.currentTimeMillis();
		}

		@Override
		public int compareTo(Object obj) {
			KeyItem item = (KeyItem) obj;
			return -1 * key.compareTo(item.key);
		}

		@Override
		public int hashCode() {
			if (StringUtils.isBlank(key)) return 0;
			return key.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null || !(obj instanceof KeyItem)) return false;
			KeyItem item = (KeyItem) obj;
			return this.key.equals(item.key);
		}
	}

	/**
	 * Key-Space Manager
	 */
	private class KeySpaceManager implements Runnable {
		private static final long TIME_INTERVAL = 10;
		private static final long CYCLE_COUNT = 6;
		private final KeySpace keySpace;

		public KeySpaceManager(KeySpace keySpace) {
			this.keySpace = keySpace;
		}

		@Override
		public void run() {
			int count = 0;
			while (true) {
				List<KeyItem> babySpace = keySpace.babySpace;
				List<KeyItem> teenageSpace = keySpace.teenageSpace;
				List<KeyItem> oldSpace = keySpace.oldSpace;

				// rearrange keys in space
				++count;
				System.out.println("######## rearranging keys > babySpace to teenageSpace, count: " + count);
				Collections.sort(babySpace);
				Collections.sort(teenageSpace);
				// move from baby to teenage
				int teenageSpaceSize = teenageSpace.size();
				for (int i = 0; i < teenageSpaceSize && i < babySpace.size(); ++i) {
					if (babySpace.get(i).count > teenageSpace.get(teenageSpaceSize - i - 1).count) {
						keySpace.upToTeenageSpace(babySpace.get(i).key);

						teenageSpace.add(0, babySpace.get(i));
						babySpace.remove(i);
						teenageSpace.remove(20);
						--i;
					}
				}
				keySpace.emptyBabySpace();

				if (count == CYCLE_COUNT) {
					count = 0;
					System.out.println("######## rearranging keys > teenage space to old space");
					// move from teenage to old
					Collections.sort(oldSpace);

				}

				try {
					TimeUnit.SECONDS.sleep(TIME_INTERVAL);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Key-Space Garbage Collector
	 */
	private class KeySpaceGC implements Runnable {
		private static final long TIME_INTERVAL = 60 * 10;
		private final KeySpace keySpace;

		public KeySpaceGC(KeySpace keySpace) {
			this.keySpace = keySpace;
		}

		@Override
		public void run() {
			while (true) {
				// garbage collecting: retiring
				System.out.println("######## garbage collecting");

				try {
					TimeUnit.SECONDS.sleep(TIME_INTERVAL);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * LRU implementation
	 */
	private class LRU<T> {
		private final Set<T> items;
		private final int SIZE;

		private LRU(int size) {
			this.SIZE = size;
			this.items = new LinkedHashSet<>(size);
		}

		private void addItem(T item) {
			if (item == null) return;
			items.remove(item);
			if (items.size() == SIZE) {
				Iterator<T> it = items.iterator();
				it.next();
				it.remove();
			}
			items.add(item);
		}
	}

	/**
	 * Utilities
	 */
	private static class Utils {
		private static final HashFunction HF = Hashing.murmur3_128(13);

		private static long calculateHash(String key) {
			return Math.abs(HF.hashBytes(key.getBytes()).asLong());
		}

		private static void arrangeHashTaskList(List<List<Integer>> hashTaskList, int taskSize) {
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
	}
}
