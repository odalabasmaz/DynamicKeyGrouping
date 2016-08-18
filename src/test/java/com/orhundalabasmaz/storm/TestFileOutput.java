package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.utils.Logger;
import org.junit.Test;

import java.io.*;

/**
 * @author Orhun Dalabasmaz
 */
public class TestFileOutput {

	@Test
	public void test() {
		File dir = new File("src/test/resources/output");
		String filename = "results-test.csv";
		String filePath = dir + "/" + filename;

		String value = "asfafa asd asda";

		try (
//				FileWriter fileWriter = new FileWriter(file.getName(), true);
//				BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
//				BufferedWriter bufferWriter = Files.newBufferedWriter(Paths.get(Resources.getResource(fileName).toURI()));
				OutputStream os = new FileOutputStream(filePath, true);
				PrintStream printStream = new PrintStream(os);
//				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "utf-8"));
		) {

			printStream.println(value);
			printStream.println(value);
			printStream.println(value);
			printStream.println(value);
			/*Files.write(Paths.get(fileName), ("#1 - " + value + "\n").getBytes(), StandardOpenOption.APPEND);

			Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/data.txt"), "utf-8"));
			writer.write("#2 - " + value + "\n");

			if (!file.exists() && !file.createNewFile()) {
				Logger.log("I/O Problem: file does not exist and not able to create one");
				return;
			}
			bufferWriter.write(value + "\n");*/

		} catch (IOException /*| URISyntaxException */ e) {
			Logger.log(e.getMessage() + " [filename: " + filename + "]");
		}
	}
}
