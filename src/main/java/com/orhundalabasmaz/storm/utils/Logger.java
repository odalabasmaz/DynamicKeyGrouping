package com.orhundalabasmaz.storm.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by orhun on 18.10.2015.
 */
public class Logger {
	private final static DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");

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
		final String time = formatter.format(System.currentTimeMillis());
		StringBuilder sb = new StringBuilder();
		sb.append("MyStorm_").append(time).append("_$ ").append(msg);
		System.out.println(sb.toString());
	}
}
