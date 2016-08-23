package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.utils.FileService;
import com.orhundalabasmaz.storm.utils.FinalResultReducer;
import com.orhundalabasmaz.storm.utils.Logger;
import com.orhundalabasmaz.storm.utils.ResultLogger;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.orhundalabasmaz.storm.utils.Constants.FINAL_RESULT_FILE_HEADER;
import static com.orhundalabasmaz.storm.utils.Constants.OUTPUT_DIR;

/**
 * @author Orhun Dalabasmaz
 */
public class Finalizer {

	@Test
	public void cleanUp() {
		Logger.log("Cucumber tests completed.");
		finalizeResults();
	}

	private void finalizeResults() {
		// remove finalResults.csv if exists
		FileService.deleteFile("output/finalResults.csv");
		ResultLogger resultLogger = new ResultLogger("finalResults.csv");
		resultLogger.log(FINAL_RESULT_FILE_HEADER);

		// read and reduce results.csv
		FinalResultReducer reducer = new FinalResultReducer();
		try {
			Files.lines(Paths.get(OUTPUT_DIR + "/" + "results.csv"))
					.skip(1)
					.forEach(reducer::reduce);
		} catch (IOException e) {
			e.printStackTrace();
		}
		reducer.eof();
	}
}
