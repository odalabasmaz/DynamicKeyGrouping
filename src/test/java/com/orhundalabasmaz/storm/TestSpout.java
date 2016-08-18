package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.loadBalancer.spouts.Data;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.Random;

/**
 * @author Orhun Dalabasmaz
 */
public class TestSpout {

	private Random rand = new Random();
	private List<Values> valuesList = Data.getValuesList();

	@Test
	public void createData() {
		Writer writer;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/data.txt"), "utf-8"));
			for (int count = 0; count < 10_000_000; ++count) {
				String data = skewnessData();
				writer.write(data + " \n");
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String skewnessData() {
		int randNum = rand.nextInt(100);
		if (randNum < 80) { //80
			return "turkey";
		} else if (randNum < 90) {
			return "spain";
		} else {
			return randomData();
		}
	}

	private String randomData() {
		int index = rand.nextInt(valuesList.size());
		return (String) valuesList.get(index).get(0);
	}
}
