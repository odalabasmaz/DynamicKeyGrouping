package com.orhundalabasmaz.storm.utils;

/**
 * @author Orhun Dalabasmaz
 */
public class FinalResultReducer {
	private String currentTestId;
	private int testCount;
	private long totalLatency;
	private long totalThroughput;
	private long totalNumberOfDistinctKeys;
	private long totalNumberOfConsumedKeys;
	private ResultLogger resultLogger;
	private ResultResolver resultResolver;

	public FinalResultReducer() {
		resultLogger = new ResultLogger("finalResults.csv");
		initialize();
	}

	private void initialize() {
		testCount = 0;
		totalLatency = 0;
		totalThroughput = 0;
		totalNumberOfDistinctKeys = 0;
		totalNumberOfConsumedKeys = 0;
	}

	public void reduce(String line) {
		resultResolver = new ResultResolver(line);
		String testId = resultResolver.getTestId();
		//"test id,date time,grouping type,data set,stream type,process duration (ms),aggregation duration (ms),number of spouts,number of worker bolts," +
		//"latency (ms),throughput (tuple),throughput ratio (tuple/sec),number of distinct keys,number of consumed keys";
		long latency = resultResolver.getLatency();
		long throughput = resultResolver.getThroughput();
		long numberOfDistinctKeys = resultResolver.getNumberOfDistinctKeys();
		long numberOfConsumedKeys = resultResolver.getNumberOfConsumedKeys();

		if (currentTestId == null) {  // first line
			setCurrentTestId(testId);
		}
		if (testId.equals(currentTestId)) {
			aggregate(latency, throughput, numberOfDistinctKeys, numberOfConsumedKeys);
		} else {
			calculateAndLogResult();
			initialize();
			setCurrentTestId(testId);
			aggregate(latency, throughput, numberOfDistinctKeys, numberOfConsumedKeys);
		}
	}

	private void setCurrentTestId(String testId) {
		currentTestId = testId;
	}

	private void aggregate(long latency, long throughput, long numberOfDistinctKeys, long numberOfConsumedKeys) {
		testCount++;
		totalLatency += latency;
		totalThroughput += throughput;
		totalNumberOfDistinctKeys += numberOfDistinctKeys;
		totalNumberOfConsumedKeys += numberOfConsumedKeys;
	}

	private void calculateAndLogResult() {
		long averageLatency = totalLatency / testCount;
		long averageThroughput = totalThroughput / testCount;
		long averageNumberOfDistinctKeys = totalNumberOfDistinctKeys / testCount;
		long averageNumberOfConsumedKeys = totalNumberOfConsumedKeys / testCount;
		double averageThroughputRatio = averageLatency > 0 ? (double) 1000 * averageThroughput / averageLatency : 0;
		double memoryConsumptionRatio = totalNumberOfDistinctKeys > 0 ? (double) totalNumberOfConsumedKeys / totalNumberOfDistinctKeys : 0;
		String value = ResultBuilder.getInstance()
				.testId(currentTestId)
				.testCount(testCount)
				.groupingType(resultResolver.getGroupingType())
				.dataSet(resultResolver.getDataSet())
				.streamType(resultResolver.getStreamType())
				.processDuration(resultResolver.getProcessDuration())
				.aggregationDuration(resultResolver.getAggregationDuration())
				.numberOfSpouts(resultResolver.getNumberOfSpouts())
				.numberOfWorkerBolts(resultResolver.getNumberOfWorkerBolts())
				.latency(averageLatency)
				.throughput(averageThroughput)
				.throughputRatio(DKGUtils.formatDoubleValue(averageThroughputRatio))
				.numberOfDistinctKeys(averageNumberOfDistinctKeys)
				.numberOfConsumedKeys(averageNumberOfConsumedKeys)
				.memoryConsumptionRatio(DKGUtils.formatDoubleValue(memoryConsumptionRatio))
				.build();
		resultLogger.log(value);
	}

	public void eof() {
		calculateAndLogResult();
		initialize();
	}
}
