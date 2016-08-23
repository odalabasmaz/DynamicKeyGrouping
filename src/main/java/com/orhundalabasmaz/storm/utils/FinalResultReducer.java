package com.orhundalabasmaz.storm.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class FinalResultReducer {
	private String previousTestId;
	private int testCount;
	private long totalLatency;
	private long totalThroughput;
	private long totalNumberOfDistinctKeys;
	private long totalNumberOfConsumedKeys;
	private ResultLogger resultLogger;
	private ResultResolver previousResultResolver, currentResultResolver;

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
		if (StringUtils.isBlank(line)) {
			return;
		}
		currentResultResolver = new ResultResolver(line);
		String currTestId = currentResultResolver.getTestId();
		//"test id,date time,grouping type,data set,stream type,process duration (ms),aggregation duration (ms),number of spouts,number of worker bolts," +
		//"latency (ms),throughput (tuple),throughput ratio (tuple/sec),number of distinct keys,number of consumed keys";
		long latency = currentResultResolver.getLatency();
		long throughput = currentResultResolver.getThroughput();
		long numberOfDistinctKeys = currentResultResolver.getNumberOfDistinctKeys();
		long numberOfConsumedKeys = currentResultResolver.getNumberOfConsumedKeys();

		if (previousTestId == null) {  // first line
			setPreviousTestId(currTestId, currentResultResolver);
		}
		if (currTestId.equals(previousTestId)) {
			aggregate(latency, throughput, numberOfDistinctKeys, numberOfConsumedKeys);
		} else {
			calculateAndLogResult();
			initialize();
			setPreviousTestId(currTestId, currentResultResolver);
			aggregate(latency, throughput, numberOfDistinctKeys, numberOfConsumedKeys);
		}
	}

	private void setPreviousTestId(String testId, ResultResolver resultResolver) {
		previousTestId = testId;
		previousResultResolver = resultResolver;
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
				.testId(previousTestId)
				.testCount(testCount)
				.groupingType(previousResultResolver.getGroupingType())
				.dataSet(previousResultResolver.getDataSet())
				.streamType(previousResultResolver.getStreamType())
				.processDuration(previousResultResolver.getProcessDuration())
				.aggregationDuration(previousResultResolver.getAggregationDuration())
				.numberOfSpouts(previousResultResolver.getNumberOfSpouts())
				.numberOfWorkerBolts(previousResultResolver.getNumberOfWorkerBolts())
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
