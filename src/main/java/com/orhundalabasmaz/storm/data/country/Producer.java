package com.orhundalabasmaz.storm.data.country;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * @author Orhun Dalabasmaz
 */
public class Producer {
	private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
	private final Random random = new Random();
	private Map<String, Integer> countryNames = new HashMap<>();
	private int countryCount;
	private FileWriter fileWriter;
	private BufferedWriter bufferedWriter;
	private int ratio;
	private int countryNameSize = Data.countryNames.size();

	public Producer(String fileNamePrefix, int ratio) {
		try {
			String fileName = "D:/cloud/data/synthetic/" + fileNamePrefix + ratio + ".txt";
			this.ratio = ratio;
			this.fileWriter = new FileWriter(fileName, false);
			this.bufferedWriter = new BufferedWriter(fileWriter);
		} catch (IOException e) {
			LOGGER.error("Exception occurred!", e);
		}
	}

	public void produce() {
		for (int i = 0; i < 10_000_000; ++i) {
			writeToFile(getCountry(ratio));
		}
		endOfFile();
	}

	public void produceHalfSkew() {
		for (int i = 0; i < 5_000_000; ++i) {
			writeToFile(getCountry(ratio));
		}
		for (int i = 0; i < 5_000_000; ++i) {
			writeToFile(getCountry(0));
		}
		endOfFile();
	}

	private String getCountry(int ratio) {
		int p = random.nextInt(100);
		String country;
		if (p < ratio) {
			country = Data.skewKey;
		} else {
			int i = random.nextInt(countryNameSize);
			country = Data.countryNames.get(i);
		}
		return country;
	}

	private void writeToFile(String country) {
		countryNames.merge(country, 1, (a, b) -> a + b);
		countryCount++;
		try {
			bufferedWriter.write(country);
			bufferedWriter.newLine();
		} catch (IOException e) {
			LOGGER.error("Exception occurred when writing to file:", e);
		}
	}

	private void endOfFile() {
		TreeMap<String, Integer> map = new TreeMap<>(countryNames);
		StringBuilder result = new StringBuilder();
		result.append("Produced ").append(countryCount).append(" country from ").append(countryNames.size())
				.append(" distinct countrie(s) with ratio ").append(ratio).append("%.").append("\n");
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			result.append(entry.getKey()).append(", ").append(entry.getValue()).append("\n");
		}
		String resultValue = result.toString();
		try {
			bufferedWriter.close();
			fileWriter.close();
		} catch (IOException e) {
			LOGGER.error("Close ex", e);
		}
		LOGGER.info(resultValue);
	}

}
