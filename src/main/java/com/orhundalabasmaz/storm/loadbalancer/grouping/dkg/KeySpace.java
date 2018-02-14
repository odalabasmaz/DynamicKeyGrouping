package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * @author Orhun Dalabasmaz
 * JVM-like architecture
 * archived / eden-space / old-space / retired
 */
public class KeySpace implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeySpace.class);
    private Map<String, KeyItem> babySpace;         // max heap
    private Map<String, KeyItem> teenageSpace;      // max heap
    private Map<String, KeyItem> oldSpace;          // max heap

    private final int OLD_MAX_SIZE;
    private final int TEENAGE_MAX_SIZE;
    private final int BABY_MAX_SIZE;

    private static final Comparator<KeyItem> DESC_COMPARATOR = (k1, k2) -> (int) (k2.getCount() - k1.getCount());

    public KeySpace() {
        this(100);
    }

    public KeySpace(int distinctKeyCounts) {
        OLD_MAX_SIZE = (int) (distinctKeyCounts * (10 / (double) 100));
        TEENAGE_MAX_SIZE = (int) (distinctKeyCounts * (40 / (double) 100));
        BABY_MAX_SIZE = (int) (distinctKeyCounts * (50 / (double) 100));

        oldSpace = new HashMap<>(OLD_MAX_SIZE);
        teenageSpace = new HashMap<>(TEENAGE_MAX_SIZE);
        babySpace = new HashMap<>(BABY_MAX_SIZE);
    }

    public void handleKey(String key, int targetTask) {
        KeyItem item = findKeyItem(key);
        if (item != null) {
            item.appearedAgain(targetTask);
        } else {
            item = new KeyItem(key, targetTask);
            babySpace.put(key, item);
        }
    }

    public KeyItem findKeyItem(String key) {
        KeyItem item = oldSpace.get(key);
        if (item != null) return item;
        item = teenageSpace.get(key);
        if (item != null) return item;
        item = babySpace.get(key);
        return item;
    }

    private KeyItem findKeyItemInList(String key, Set<KeyItem> keyItemList) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        for (KeyItem item : keyItemList) {
            if (item == null) {
                LOGGER.error("item cannot be null!");
                continue;
            }
            if (key.equals(item.getKey())) {
                return item;
            }
        }
        return null;
    }

    public boolean inBabySpace(String key) {
        return babySpace.containsKey(key);
    }

    public boolean inTeenageSpace(String key) {
        return teenageSpace.containsKey(key);
    }

    public boolean inOldSpace(String key) {
        return oldSpace.containsKey(key);
    }

    public void promoteToTeenageSpace() {
        LOGGER.debug("promoteToTeenageSpace");
        // convert to list
        List<KeyItem> babyList = new ArrayList<>(babySpace.values());
        List<KeyItem> teenageList = new ArrayList<>(teenageSpace.values());
        babyList.sort(DESC_COMPARATOR);
        babyList = DKGUtils.truncateList(babyList, BABY_MAX_SIZE);
        teenageList.sort(DESC_COMPARATOR);
        promoteToNextSpace(babyList, teenageList, TEENAGE_MAX_SIZE);

        // back to set
        babySpace.clear();
        DKGUtils.putListIntoMap(babyList, babySpace);
        teenageSpace.clear();
        DKGUtils.putListIntoMap(teenageList, teenageSpace);
    }

    public void promoteToOldSpace() {
        LOGGER.debug("promoteToOldSpace");
        // convert to list
        List<KeyItem> teenageList = new ArrayList<>(teenageSpace.values());
        List<KeyItem> oldList = new ArrayList<>(oldSpace.values());
        teenageList.sort(DESC_COMPARATOR);
        oldList.sort(DESC_COMPARATOR);
        promoteToNextSpace(teenageList, oldList, OLD_MAX_SIZE);
        printKeyItems(oldList);

        // back to set
        teenageSpace.clear();
        DKGUtils.putListIntoMap(teenageList, teenageSpace);
        oldSpace.clear();
        DKGUtils.putListIntoMap(oldList, oldSpace);
    }

    private void printKeyItems(List<KeyItem> keyItems) {
        keyItems.sort(DESC_COMPARATOR);
        StringBuilder builder = new StringBuilder();
        keyItems.forEach(item -> builder
                .append(item.getKey()).append(",")
                .append(item.getCount()).append(",")
                .append(item.getTargetTasksCount()).append("\n"));
        LOGGER.info("#HOTTEST KEYS:\n{}", builder);
    }

    private void promoteToNextSpace(List<KeyItem> fromSpace, List<KeyItem> toSpace, int toSpaceMaxSize) {
        int loop = 0;
        while (toSpace.size() < toSpaceMaxSize) {
            if (fromSpace.isEmpty()) return;
            KeyItem firstBaby = fromSpace.get(0);
            fromSpace.remove(0);
            toSpace.add(0, firstBaby);
            ++loop;
        }
        //todo: should reorder the toSpace ??

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

    public void gc() {
        long now = System.currentTimeMillis();
        long threshold = 60 * 60 * 1000;

        clearSpace(oldSpace, now - threshold);
        clearSpace(teenageSpace, now - threshold);
    }

    private void clearSpace(Map<String, KeyItem> space, long threshold) {
        for (Map.Entry<String, KeyItem> entry : space.entrySet()) {
            String key = entry.getKey();
            KeyItem value = entry.getValue();
            if (value.getLastSeen() < threshold) {
                space.remove(key);
            }
        }
    }
}
