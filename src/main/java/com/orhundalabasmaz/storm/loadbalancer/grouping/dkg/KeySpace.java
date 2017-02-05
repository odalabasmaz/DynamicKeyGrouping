package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import com.orhundalabasmaz.storm.utils.CustomLogger;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author Orhun Dalabasmaz
 *         JVM-like architecture
 *         archived / eden-space / old-space / retired
 */
public class KeySpace implements Serializable {
	private List<KeyItem> babySpace;         // max heap
	private List<KeyItem> teenageSpace;      // max heap
	private List<KeyItem> oldSpace;          // max heap

	private final int OLD_MAX_SIZE;
	private final int TEENAGE_MAX_SIZE;
	private final int BABY_MAX_SIZE;

	private static final Comparator<KeyItem> DESC_COMPARATOR = new Comparator<KeyItem>() {
		@Override
		public int compare(KeyItem k1, KeyItem k2) {
			return (int) (k2.getCount() - k1.getCount());
		}
	};

	public KeySpace() {
		OLD_MAX_SIZE = 10;
		TEENAGE_MAX_SIZE = 20;
		BABY_MAX_SIZE = 50;

		oldSpace = new ArrayList<>(OLD_MAX_SIZE);
		teenageSpace = new ArrayList<>(TEENAGE_MAX_SIZE);
		babySpace = new ArrayList<>(BABY_MAX_SIZE);
	}

	public KeySpace(int distinctKeyCounts) {
		OLD_MAX_SIZE = (int) (distinctKeyCounts * (10 / (double) 100));
		TEENAGE_MAX_SIZE = (int) (distinctKeyCounts * (40 / (double) 100));
		BABY_MAX_SIZE = (int) (distinctKeyCounts * (50 / (double) 100));

		oldSpace = new ArrayList<>(OLD_MAX_SIZE);
		teenageSpace = new ArrayList<>(TEENAGE_MAX_SIZE);
		babySpace = new ArrayList<>(BABY_MAX_SIZE);
	}

	public void handleKey(String key) {
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
		if (StringUtils.isBlank(key)) {
			return null;
		}
		for (KeyItem item : keyItemList) {
			if (item == null) {
				CustomLogger.error("item cannot be null!");
				continue;
			}
			if (key.equals(item.getKey())) {
				return item;
			}
		}
		return null;
	}

	public boolean inBabySpace(String key) {
		return findKeyItemInList(key, babySpace) != null;
	}

	public boolean inTeenageSpace(String key) {
		return findKeyItemInList(key, teenageSpace) != null;
	}

	public boolean inOldSpace(String key) {
		return findKeyItemInList(key, oldSpace) != null;
	}

	public void upToTeenageSpace() {
		upToNextSpace(babySpace, teenageSpace, TEENAGE_MAX_SIZE);
		/*int loop = 0;
		for (; teenageSpace.size() < TEENAGE_MAX_SIZE; ++loop) {
			if (babySpace.isEmpty()) return;
			KeyItem firstBaby = babySpace.get(0);
			babySpace.remove(0);
			teenageSpace.add(0, firstBaby);
		}

		for (; loop < TEENAGE_MAX_SIZE; ++loop) {
			if (babySpace.isEmpty()) return;
			KeyItem firstBaby = babySpace.get(0);
			int lastTeenIndex = teenageSpace.size() - 1;
			KeyItem lastTeen = teenageSpace.get(lastTeenIndex);
			if (firstBaby.getCount() > lastTeen.getCount()) {
				babySpace.remove(0);
				teenageSpace.remove(lastTeenIndex);
				teenageSpace.add(0, firstBaby);
				babySpace.add(lastTeen);
			} else {
				break;
			}
		}*/
	}

	public void upToOldSpace() {
		upToNextSpace(teenageSpace, oldSpace, OLD_MAX_SIZE);
	}

	private void upToNextSpace(List<KeyItem> fromSpace, List<KeyItem> toSpace, int toSpaceMaxSize) {
		int loop = 0;
		for (; toSpace.size() < toSpaceMaxSize; ++loop) {
			if (fromSpace.isEmpty()) return;
			KeyItem firstBaby = fromSpace.get(0);
			fromSpace.remove(0);
			toSpace.add(0, firstBaby);
		}

		for (; loop < toSpaceMaxSize; ++loop) {
			if (fromSpace.isEmpty()) return;
			KeyItem firstBaby = fromSpace.get(0);
			int lastTeenIndex = toSpace.size() - 1;
			KeyItem lastTeen = toSpace.get(lastTeenIndex);
			if (firstBaby.getCount() > lastTeen.getCount()) {
				fromSpace.remove(0);
				toSpace.remove(lastTeenIndex);
				toSpace.add(0, firstBaby);
				fromSpace.add(lastTeen);
			} else {
				break;
			}
		}
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

	public void sortBabySpace() {
		Collections.sort(babySpace, DESC_COMPARATOR);
	}

	public void sortTeenageSpace() {
		Collections.sort(teenageSpace, DESC_COMPARATOR);
	}

	public void sortOldSpace() {
		Collections.sort(oldSpace, DESC_COMPARATOR);
	}

	public void truncateBabySpace() {
		if (babySpace.size() > BABY_MAX_SIZE)
			babySpace = babySpace.subList(0, BABY_MAX_SIZE);
	}
}
