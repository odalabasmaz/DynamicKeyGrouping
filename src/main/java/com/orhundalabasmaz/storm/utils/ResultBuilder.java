package com.orhundalabasmaz.storm.utils;

import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.common.StreamType;

/**
 * @author Orhun Dalabasmaz
 */
public class ResultBuilder {
	private String result;

	private ResultBuilder() {
		result = "";
	}

	public static ResultBuilder getInstance() {
		return new ResultBuilder();
	}

	public ResultBuilder testId(String value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder testCount(int value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder datetime(String value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder throughput(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder throughputRatio(String value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder latency(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder numberOfDistinctKeys(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder numberOfConsumedKeys(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder groupingType(GroupingType value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder dataSet(String value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder streamType(StreamType value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder processDuration(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder aggregationDuration(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder numberOfSpouts(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder numberOfWorkerBolts(long value) {
		this.result += "," + value;
		return this;
	}

	public ResultBuilder memoryConsumptionRatio(String value) {
		this.result += "," + value;
		return this;
	}

	public String build() {
		return result.substring(1);
	}
}
