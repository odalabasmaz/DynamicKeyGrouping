package com.orhundalabasmaz.storm.config;

import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.common.StreamType;

/**
 * @author Orhun Dalabasmaz
 */
public class RuntimeConfig {
	private String dataSet;
	private StreamType streamType;
	private GroupingType groupingType;
	private Long processDuration;
	private Long aggregationDuration;
	private Long terminationDuration;
	private Integer spoutCount;
	private Integer workerCount;
	private Integer retryCount;

	/* getters & setters */
	public String getDataSet() {
		return dataSet;
	}

	public void setDataSet(String dataSet) {
		this.dataSet = dataSet;
	}

	public StreamType getStreamType() {
		return streamType;
	}

	public void setStreamType(StreamType streamType) {
		this.streamType = streamType;
	}

	public GroupingType getGroupingType() {
		return groupingType;
	}

	public void setGroupingType(GroupingType groupingType) {
		this.groupingType = groupingType;
	}

	public Long getProcessDuration() {
		return processDuration;
	}

	public void setProcessDuration(Long processDuration) {
		this.processDuration = processDuration;
	}

	public Long getAggregationDuration() {
		return aggregationDuration;
	}

	public void setAggregationDuration(Long aggregationDuration) {
		this.aggregationDuration = aggregationDuration;
	}

	public Long getTerminationDuration() {
		return terminationDuration;
	}

	public void setTerminationDuration(Long terminationDuration) {
		this.terminationDuration = terminationDuration;
	}

	public Integer getSpoutCount() {
		return spoutCount;
	}

	public void setSpoutCount(Integer spoutCount) {
		this.spoutCount = spoutCount;
	}

	public Integer getWorkerCount() {
		return workerCount;
	}

	public void setWorkerCount(Integer workerCount) {
		this.workerCount = workerCount;
	}

	public Integer getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(Integer retryCount) {
		this.retryCount = retryCount;
	}
}
