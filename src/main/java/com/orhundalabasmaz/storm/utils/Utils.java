package com.orhundalabasmaz.storm.utils;

import java.util.UUID;

/**
 * @author Orhun Dalabasmaz
 */
public class Utils {

	public static String generateUUID() {
		return UUID.randomUUID().toString().toUpperCase();
	}

	public static long getCurrentTimestamp() {
		return System.currentTimeMillis();
	}

}
