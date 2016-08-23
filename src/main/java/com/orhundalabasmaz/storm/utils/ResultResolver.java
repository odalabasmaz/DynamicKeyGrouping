package com.orhundalabasmaz.storm.utils;

import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.StreamType;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class ResultResolver {
	// inputs
	private String testId;
	private String datetime;
	private GroupingType groupingType;
	private String dataSet;
	private StreamType streamType;
	private long processDuration;
	private long aggregationDuration;
	private long numberOfSpouts;
	private long numberOfWorkerBolts;
	// outputs
	private long latency;
	private long throughput;
	private double throughputRatio;
	private long numberOfDistinctKeys;
	private long numberOfConsumedKeys;

	public ResultResolver(String line) {
		this.resolve(line);
	}

	private void resolve(String line) {
		if (StringUtils.isBlank(line)) {
			return;
		}
		String[] token = line.split(",");
		testId = token[0];
		datetime = token[1];
		groupingType = GroupingType.valueOf(token[2]);
		dataSet = token[3];
		streamType = StreamType.valueOf(token[4]);
		processDuration = Long.valueOf(token[5]);
		aggregationDuration = Long.valueOf(token[6]);
		numberOfSpouts = Long.valueOf(token[7]);
		numberOfWorkerBolts = Long.valueOf(token[8]);
		latency = Long.valueOf(token[9]);
		throughput = Long.valueOf(token[10]);
		throughputRatio = Double.valueOf(token[11]);
		numberOfDistinctKeys = Long.valueOf(token[12]);
		numberOfConsumedKeys = Long.valueOf(token[13]);
	}

	/* getters */
	public String getTestId() {
		return testId;
	}

	public String getDatetime() {
		return datetime;
	}

	public GroupingType getGroupingType() {
		return groupingType;
	}

	public String getDataSet() {
		return dataSet;
	}

	public StreamType getStreamType() {
		return streamType;
	}

	public long getProcessDuration() {
		return processDuration;
	}

	public long getAggregationDuration() {
		return aggregationDuration;
	}

	public long getNumberOfSpouts() {
		return numberOfSpouts;
	}

	public long getNumberOfWorkerBolts() {
		return numberOfWorkerBolts;
	}

	public long getLatency() {
		return latency;
	}

	public long getThroughput() {
		return throughput;
	}

	public double getThroughputRatio() {
		return throughputRatio;
	}

	public long getNumberOfDistinctKeys() {
		return numberOfDistinctKeys;
	}

	public long getNumberOfConsumedKeys() {
		return numberOfConsumedKeys;
	}
}
