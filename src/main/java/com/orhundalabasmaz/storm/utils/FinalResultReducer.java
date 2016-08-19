package com.orhundalabasmaz.storm.utils;

/**
 * @author Orhun Dalabasmaz
 */
public class FinalResultReducer {
	private String currentTestId;
	private int count;
	private long totalTimeConsumption;
	private long totalThroughput;
	private long totalNumberOfDistinctKeys;
	private long totalNumberOfConsumedKeys;
	private ResultLogger resultLogger;

	public FinalResultReducer() {
		resultLogger = new ResultLogger("finalResults.csv");
		initialize();
	}

	private void initialize() {
		count = 0;
		totalTimeConsumption = 0;
		totalThroughput = 0;
		totalNumberOfDistinctKeys = 0;
		totalNumberOfConsumedKeys = 0;
	}

	public void reduce(String line) {
		String[] token = line.split(",");
		// ignore datetime token[0]
		String testId = token[1];
		long timeConsumption = Long.valueOf(token[2]);
		long throughput = Long.valueOf(token[3]);
		int numberOfDistinctKeys = Integer.valueOf(token[4]);
		int numberOfConsumedKeys = Integer.valueOf(token[5]);

		if (currentTestId == null) {  // first line
			setCurrentTestId(testId);
		}
		if (testId.equals(currentTestId)) {
			aggregate(timeConsumption, throughput, numberOfDistinctKeys, numberOfConsumedKeys);
		} else {
			calculateAndLogResult();
			initialize();
			setCurrentTestId(testId);
			aggregate(timeConsumption, throughput, numberOfDistinctKeys, numberOfConsumedKeys);
		}
	}

	private void setCurrentTestId(String testId) {
		currentTestId = testId;
	}

	private void aggregate(long timeConsumption, long throughput, int numberOfDistinctKeys, int numberOfConsumedKeys) {
		count++;
		totalTimeConsumption += timeConsumption;
		totalThroughput += throughput;
		totalNumberOfDistinctKeys += numberOfDistinctKeys;
		totalNumberOfConsumedKeys += numberOfConsumedKeys;
	}

	private void calculateAndLogResult() {
		long averageTimeConsumption = totalTimeConsumption / count;
		long averageThroughput = totalThroughput / count;
		long averageNumberOfDistinctKeys = totalNumberOfDistinctKeys / count;
		long averageNumberOfConsumedKeys = totalNumberOfConsumedKeys / count;
		double memoryConsumptionRatio = totalNumberOfDistinctKeys > 0 ? (double) totalNumberOfConsumedKeys / totalNumberOfDistinctKeys : 0;
		String value = currentTestId + "," + count + "," + averageTimeConsumption + "," + averageThroughput + "," +
				averageNumberOfDistinctKeys + "," + averageNumberOfConsumedKeys + "," +
				DKGUtils.formatDoubleValue(memoryConsumptionRatio);
		resultLogger.log(value);
	}

	public void eof() {
		calculateAndLogResult();
		initialize();
	}
}
