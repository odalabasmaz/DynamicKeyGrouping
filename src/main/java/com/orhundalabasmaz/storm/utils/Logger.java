package com.orhundalabasmaz.storm.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class Logger {
	// todo: level
	private final static boolean DEBUG_ENABLED = true;
	private final static boolean INFO_ENABLED = false;
	private final static boolean ERROR_ENABLED = true;

	public static void log(String msg) {
		if (DEBUG_ENABLED) {
			print("DEBUG", msg);
		}
	}

	public static void info(String msg) {
		if (INFO_ENABLED) {
			print("INFO", msg);
		}
	}

	public static void error(String msg) {
		if (ERROR_ENABLED) {
			print("ERROR", msg);
		}
	}

	private static synchronized void print(String label, String msg) {
		if (StringUtils.isBlank(msg)) {
			System.out.println();
			return;
		}
		String time = DKGUtils.getCurrentDatetime();
		System.out.println("MyStorm [" + time + "][" + label + "] " + msg);
	}
}
