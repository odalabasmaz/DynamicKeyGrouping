package com.orhundalabasmaz.storm.cucumber;

import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.StreamType;

/**
 * @author Orhun Dalabasmaz
 */
public class RuntimeConfig {
	private String dataType;
	private StreamType streamType;
	private GroupingType groupingType;
	private Long processDuration;
	private Long aggregationDuration;
	private Long terminationTimeout;
	private Integer spoutCount;
	private Integer workerCount;

	/* getters & setters */
	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
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

	public Long getTerminationTimeout() {
		return terminationTimeout;
	}

	public void setTerminationTimeout(Long terminationTimeout) {
		this.terminationTimeout = terminationTimeout;
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
}
