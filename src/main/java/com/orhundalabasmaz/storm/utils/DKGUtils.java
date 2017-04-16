package com.orhundalabasmaz.storm.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.orhundalabasmaz.storm.loadbalancer.grouping.dkg.KeyItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Orhun Dalabasmaz
 */
public class DKGUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(DKGUtils.class);
	private static final Format DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static final HashFunction HF = Hashing.murmur3_128(13);
	private static final Random random = new Random();

	private DKGUtils() {
	}

	public static long calculateHash(String key) {
		return Math.abs(HF.hashBytes(key.getBytes()).asLong());
	}

	public static void arrangeHashTaskList(List<List<Integer>> hashTaskList, int taskSize) {
		// 0 -> 1 2 3
		// 1 -> 1 3 2
		// ...
		List<Integer> domain = new ArrayList<>(taskSize);
		for (int i = 0; i < taskSize; ++i) {
			domain.add(i);
		}
		calcPerms(hashTaskList, domain, new ArrayList<Integer>(taskSize));
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

	public static void sleepInNanoseconds(long duration) {
		try {
			TimeUnit.NANOSECONDS.sleep(duration);
		} catch (InterruptedException e) {
			LOGGER.error("Exception occurred when thread sleeping", e);
		}
	}

	public static void sleepInMicroseconds(long duration) {
		try {
			TimeUnit.MICROSECONDS.sleep(duration);
		} catch (InterruptedException e) {
			LOGGER.error("Exception occurred when thread sleeping", e);
		}
	}

	public static void sleepInMilliseconds(long duration) {
		try {
			TimeUnit.MILLISECONDS.sleep(duration);
		} catch (InterruptedException e) {
			LOGGER.error("Exception occurred when thread sleeping", e);
		}
	}

	public static void sleepInSeconds(long duration) {
		try {
			TimeUnit.SECONDS.sleep(duration);
		} catch (InterruptedException e) {
			LOGGER.error("Exception occurred when thread sleeping", e);
		}
	}

	public static long getCurrentTimestamp() {
		return System.currentTimeMillis();
	}

	public static String getCurrentDatetime() {
		return DATE_FORMATTER.format(System.currentTimeMillis());
	}

	public static String formattedTime(long time) {
		return DATE_FORMATTER.format(time);
	}

	public static String generateTestId() {
		return UUID.randomUUID().toString();
	}

	public static String generateUUID() {
		return UUID.randomUUID().toString().toUpperCase();
	}

	public static String formatDoubleValue(double value) {
		return String.format(Locale.US, "%.2f", value);
	}

	public static void beep() {
		beep(1);
	}

	private static void beep(int count) {
		for (int i = 0; i < count; ++i) {
			Toolkit.getDefaultToolkit().beep();
		}
	}

	public static int rand(int bound) {
		return random.nextInt(bound);
	}

	public static <T> List<T> truncateList(List<T> list, int size) {
		return list.size() > size ? list.subList(0, size) : list;
	}

	public static void putListIntoMap(List<KeyItem> items, Map<String, KeyItem> map) {
		for (KeyItem item : items) {
			map.put(item.getKey(), item);
		}
	}

	public static Map<String, Long> sortByValue(Map<String, Long> unsortMap) {
		List<Map.Entry<String, Long>> list = new LinkedList<>(unsortMap.entrySet());
		list.sort((o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
		Map<String, Long> sortedMap = new LinkedHashMap<>();
		for (Map.Entry<String, Long> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
