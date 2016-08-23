package com.orhundalabasmaz.storm.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.awt.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Orhun Dalabasmaz
 */
public class DKGUtils {
	private static final DateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static final HashFunction HF = Hashing.murmur3_128(13);

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

	public static void sleepInMicroseconds(long duration) {
		try {
			TimeUnit.MICROSECONDS.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void sleepInMilliseconds(long duration) {
		try {
			TimeUnit.MILLISECONDS.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void sleepInSeconds(long duration) {
		try {
			TimeUnit.SECONDS.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static String getCurrentDatetime() {
		return DATE_FORMATTER.format(System.currentTimeMillis());
	}

	public static String generateTestId() {
		return UUID.randomUUID().toString();
	}

	public static String formatDoubleValue(double value) {
		return String.format(Locale.US, "%.2f", value);
	}

	public static void beep() {
		beep(1);
	}

	public static void beep(int count) {
		for (int i = 0; i < count; ++i) {
			Toolkit.getDefaultToolkit().beep();
		}
	}
}
