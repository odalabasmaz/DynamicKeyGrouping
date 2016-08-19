package com.orhundalabasmaz.storm.utils;

import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;

import java.io.*;

/**
 * @author Orhun Dalabasmaz
 */
public class ResultLogger implements Serializable {
	private static File dir = new File("src/test/resources/output");
	private static String filename = "results.csv";
	private static String filePath = dir + "/" + filename;
	private static boolean append = true;

	public static void log(String value) {
		log(value, true);
	}

	public static void log(String value, boolean timestamp) {
		try (
				OutputStream os = new FileOutputStream(filePath, append);
				PrintStream printStream = new PrintStream(os);
		) {
			if (timestamp) {
				value = DKGUtils.getCurrentDatetime() + "," + value;
			}
			printStream.println(value);
		} catch (IOException e) {
			Logger.log(e.getMessage() + " [file: " + filePath + "]");
		}
	}
}
