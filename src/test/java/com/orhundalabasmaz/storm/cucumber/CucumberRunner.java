package com.orhundalabasmaz.storm.cucumber;

import com.orhundalabasmaz.storm.utils.FileService;
import com.orhundalabasmaz.storm.utils.Logger;
import com.orhundalabasmaz.storm.utils.ResultLogger;
import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author Orhun Dalabasmaz
 */
@CucumberOptions(features = "src/test/resources/cucumber/", tags = "@all")
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
		String header = "date time,test id,time consumption,throughput,number of distinct keys,number of consumed keys";
		ResultLogger.log(header, false);
	}

	@AfterClass
	public static void cleanUp() {
		Logger.log("Cucumber tests completed.");
	}
}
