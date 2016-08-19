package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.utils.FileService;
import com.orhundalabasmaz.storm.utils.FinalResultReducer;
import com.orhundalabasmaz.storm.utils.Logger;
import com.orhundalabasmaz.storm.utils.ResultLogger;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.orhundalabasmaz.storm.utils.Constants.OUTPUT_DIR;

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

	@Test
	public void finalizeResults() throws IOException {
		// remove finalResults.csv if exists
		FileService.deleteFile("output/finalResults.csv");
		ResultLogger resultLogger = new ResultLogger("finalResults.csv");
		String header = "test id,test count,time consumption,throughput,number of distinct keys,number of consumed keys,memory consumption ratio";
		resultLogger.log(header);

		// read and reduce results.csv
		FinalResultReducer reducer = new FinalResultReducer();
		Files.lines(Paths.get(OUTPUT_DIR + "/" + "results.csv"))
				.skip(1)
				.forEach(reducer::reduce);
		reducer.eof();
	}
}
