package com.orhundalabasmaz.storm.utils;

import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DKGUtils;

import java.io.*;

/**
 * @author Orhun Dalabasmaz
 */
public class ResultLogger {
	private File dir = new File("src/test/resources/output");
	private String filename = "results.csv";
	private String filePath = dir + "/" + filename;
	private boolean append = true;

	public void log(String value) {
		try (
				OutputStream os = new FileOutputStream(filePath, append);
				PrintStream printStream = new PrintStream(os);
		) {
			printStream.println(DKGUtils.getCurrentDatetime() + "," + value);
		} catch (IOException e) {
			Logger.log(e.getMessage() + " [file: " + filePath + "]");
		}
	}
}
