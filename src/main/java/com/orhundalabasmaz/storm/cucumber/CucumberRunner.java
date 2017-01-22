package com.orhundalabasmaz.storm.cucumber;

/*import com.orhundalabasmaz.storm.utils.*;
import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.orhundalabasmaz.storm.utils.Constants.*;*/

/**
 * @author Orhun Dalabasmaz
 */
/*
@CucumberOptions(features = "src/test/resources/cucumber/", tags = "@skew")
@RunWith(Cucumber.class)
public class CucumberRunner {

	@BeforeClass
	public static void initialize() {
		Logger.log("Cucumber tests started.");
		Logger.log("initializing output...");
		initializeOutput();
	}

	private static void initializeOutput() {
		// mkdir resources/output if not exists
		FileService.createDirectory("output");
		// rm file results.csv if exists
		FileService.deleteFile("output/results.csv");
		// init header of content
		ResultLogger resultLogger = new ResultLogger("results.csv");
		resultLogger.log(RESULT_FILE_HEADER);
	}

	@AfterClass
	public static void cleanUp() {
		Logger.log("Cucumber tests completed.");
		finalizeResults();
		DKGUtils.beep(3);
	}

	private static void finalizeResults() {
		// remove finalResults.csv if exists
		FileService.deleteFile("output/finalResults.csv");
		ResultLogger resultLogger = new ResultLogger("finalResults.csv");
		resultLogger.log(FINAL_RESULT_FILE_HEADER);

		// read and reduce results.csv
		*/
/*FinalResultReducer reducer = new FinalResultReducer();
		try {
			Files.lines(Paths.get(OUTPUT_DIR + "/" + "results.csv"))
					.skip(1)
					.forEach(reducer::reduce);
		} catch (IOException e) {
			e.printStackTrace();
		}
		reducer.eof();*//*

	}
}
*/
