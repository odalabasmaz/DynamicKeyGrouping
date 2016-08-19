package com.orhundalabasmaz.storm.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class Logger {
	// todo: level
	private final static boolean LOG_ENABLED = true;
	private final static boolean INFO_ENABLED = false;

	public static void log(String msg) {
		if (LOG_ENABLED) {
			print(msg);
		}
	}

	public static void info(String msg) {
		if (INFO_ENABLED) {
			print(msg);
		}
	}

	private static synchronized void print(String msg) {
		if (StringUtils.isBlank(msg)) {
			System.out.println();
			return;
		}
		String time = DKGUtils.getCurrentDatetime();
		System.out.println("MyStorm_" + time + "_$ " + msg);
	}
}
