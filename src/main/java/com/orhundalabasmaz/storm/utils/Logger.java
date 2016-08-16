package com.orhundalabasmaz.storm.utils;

import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;
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
		StringBuilder sb = new StringBuilder();
		sb.append("MyStorm_").append(time).append("_$ ").append(msg);
		System.out.println(sb.toString());
	}
}
