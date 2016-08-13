package com.orhundalabasmaz.storm.loadBalancer.grouping.archieve;

import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.KeyItem;
import com.orhundalabasmaz.storm.utils.Depque;
import com.orhundalabasmaz.storm.utils.IntervalHeap;

import java.io.Serializable;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * @author Orhun Dalabasmaz
 *         JVM-like architecture
 *         archived / eden-space / old-space / retired
 */
public class KeySpace_v1 implements Serializable {
	private final Queue<KeyItem> babySpace;         // min heap
	private final Depque<KeyItem> teenageSpace;     // max heap
	private final Queue<KeyItem> oldSpace;          // max heap

	private final int BABY_MAX_SIZE = 50;
	private final int TEENAGE_MAX_SIZE = 20;
	private final int OLD_MAX_SIZE = 10;

	public KeySpace_v1() {
		babySpace = new PriorityQueue<>(BABY_MAX_SIZE, (Comparator<KeyItem> & Serializable) (n1, n2) -> (int) (n2.getCount() - n1.getCount()));
//		teenageSpace = new PriorityQueue<>(TEENAGE_MAX_SIZE, (Comparator<KeyItem> & Serializable) (n1, n2) -> (int) (n2.getCount() - n1.getCount()));
		teenageSpace = new IntervalHeap<>(TEENAGE_MAX_SIZE, (Comparator<KeyItem> & Serializable) (n1, n2) -> (int) (n1.getCount() - n2.getCount()));
		oldSpace = new PriorityQueue<>(OLD_MAX_SIZE, (Comparator<KeyItem> & Serializable) (n1, n2) -> (int) (n1.getCount() - n2.getCount()));
	}

	public Queue<KeyItem> getBabySpace() {
		return babySpace;
	}

	public Depque<KeyItem> getTeenageSpace() {
		return teenageSpace;
	}

	public Queue<KeyItem> getOldSpace() {
		return oldSpace;
	}

	public void handleKey(String key) {
		KeyItem item = findKeyItem(key);
		if (item != null) {
			item.appearedAgain();
			rearrangeQueue(item);
		} else {
			item = new KeyItem(key);
			babySpace.offer(item);
		}
	}

	private void rearrangeQueue(KeyItem item) {
		// remove and add item again in order to rearrange queue
		if (oldSpace.remove(item)) oldSpace.offer(item);
		if (teenageSpace.remove(item)) teenageSpace.offer(item);
		if (babySpace.remove(item)) babySpace.offer(item);
	}

	private KeyItem findKeyItem(String key) {
		KeyItem item;
		item = findKeyItemInQueue(key, oldSpace);
		if (item != null) return item;
		item = findKeyItemInQueue(key, teenageSpace);
		if (item != null) return item;
		item = findKeyItemInQueue(key, babySpace);
		if (item != null) return item;
		return null;
	}

	private KeyItem findKeyItemInQueue(String key, Queue<KeyItem> keyItemQueue) {
		for (KeyItem item : keyItemQueue) {
			if (item.getKey().equals(key)) {
				return item;
			}
		}
		return null;
	}

	public boolean inBabySpace(String key) {
		KeyItem item = findKeyItemInQueue(key, babySpace);
		return item != null;
	}

	public boolean inTeenageSpace(String key) {
		KeyItem item = findKeyItemInQueue(key, teenageSpace);
		return item != null;
	}

	public boolean inOldSpace(String key) {
		KeyItem item = findKeyItemInQueue(key, oldSpace);
		return item != null;
	}

	public void downToTeenageSpace() {

	}

	public boolean upToTeenageSpace() {
//		return upToNextSpace(babySpace, teenageSpace, TEENAGE_MAX_SIZE);
		KeyItem fromItem = babySpace.peek();
		KeyItem toItem = teenageSpace.peekLast();
		if (fromItem == null) {
			return false;
		} else if (teenageSpace.size() < TEENAGE_MAX_SIZE) {
			babySpace.poll();
			teenageSpace.offer(fromItem);
			return true;
		} else if (fromItem.getCount() > toItem.getCount()) {
			babySpace.poll();
			teenageSpace.pollLast();
			babySpace.offer(toItem);
			teenageSpace.offer(fromItem);
			return true;
		}
		return false;
	}

	public boolean upToOldSpace() {
//		return upToNextSpace(teenageSpace, oldSpace, OLD_MAX_SIZE);
		KeyItem fromItem = teenageSpace.peekFirst();
		KeyItem toItem = oldSpace.peek();
		if (fromItem == null) {
			return false;
		} else if (oldSpace.size() < OLD_MAX_SIZE) {
			teenageSpace.poll();
			oldSpace.offer(fromItem);
			return true;
		} else if (fromItem.getCount() > toItem.getCount()) {
			teenageSpace.poll();
			oldSpace.poll();
			teenageSpace.offer(toItem);
			oldSpace.offer(fromItem);
			return true;
		}
		return false;
	}

	private boolean upToNextSpace(Queue<KeyItem> fromSpace, Queue<KeyItem> toSpace, int maxSpaceSize) {
		KeyItem fromItem = fromSpace.peek();
		KeyItem toItem = toSpace.peek();
		if (fromItem == null) {
			return false;
		} else if (toSpace.size() < maxSpaceSize) {
			fromSpace.poll();
			toSpace.offer(fromItem);
			return true;
		} else if (fromItem.getCount() > toItem.getCount()) {
			fromSpace.poll();
			toSpace.poll();
			fromSpace.offer(toItem);
			toSpace.offer(fromItem);
			return true;
		}
		return false;
	}

	public void archive(String key) {

	}

	public void retire(String key) {

	}

	public void emptyBabySpace() {
		babySpace.clear();
	}

	public int getBabySize() {
		return babySpace.size();
	}

	public int getBabyMaxSize() {
		return BABY_MAX_SIZE;
	}

	public int getTeenageSize() {
		return teenageSpace.size();
	}

	public int getTeenageMaxSize() {
		return TEENAGE_MAX_SIZE;
	}

	public int getOldSize() {
		return oldSpace.size();
	}

	public int getOldMaxSize() {
		return OLD_MAX_SIZE;
	}
}
