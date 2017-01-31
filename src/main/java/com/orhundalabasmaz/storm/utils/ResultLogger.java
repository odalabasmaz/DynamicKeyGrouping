package com.orhundalabasmaz.storm.utils;

import java.io.*;

/**
 * @author Orhun Dalabasmaz
 */
public class ResultLogger implements Serializable {
	private String filename;
	private String filePath;
	private boolean append;

	public ResultLogger(String filename) {
		this(filename, true);
	}

	public ResultLogger(String filename, boolean append) {
		this.filename = filename;
		this.append = append;
		this.filePath = "/" + filename;
	}

	public void log(String value) {
		try (
				OutputStream os = new FileOutputStream(filePath, append);
				PrintStream printStream = new PrintStream(os);
		) {
			printStream.println(value);
			System.out.println(">> " + value);
		} catch (IOException e) {
			CustomLogger.log(e.getMessage() + " [file: " + filename + "]");
		}
	}
}
