package com.orhundalabasmaz.storm.config;

import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.DataType;

import java.io.Serializable;

/**
 * @author Orhun Dalabasmaz
 */
public class Configuration implements Serializable {
	// APP VERSION
	public String appVersion = "v1.0";

	// NUMBER OF PROCESS UNITS
	public int numberOfWorkers = 1;
	public int numberOfSpouts = 1;
	public int numberOfSplitterBolts = 3;
	public int numberOfWorkerBolts = 50;
	public int numberOfAggregatorBolts = 1;
	public int numberOfResultBolts = 1;
	public int numberOfTasks = 2;

	// TIME INTERVAL
	public long timeIntervalOfDataStreams = 1;          // ms (default 1 ms)
	public long timeIntervalOfWorkerBolts = 5;          // sec
	public long timeIntervalOfAggregatorBolts = 15;     // sec

	public long terminationTimeout = 1 * 60 * 1000;                 // ms
	//	public long TOPOLOGY_TIMEOUT = 5 * 10 * 60 * 1000;          // ms
	public long topologyTimeout = terminationTimeout + 10_000;      // ms

	// RUNTIME PROPS
	public DataType dataType = DataType.HOMOGENEOUS;
	public GroupingType groupingType = GroupingType.DYNAMIC_KEY;
	public AggregatorType aggregatorType = AggregatorType.CUMULATIVE;
	public long processDuration = 10; //ms
	public long aggregationDuration = 0; //ms
	public boolean isLogEnabled = true;

	/* getters & setters */
	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

	public int getNumberOfWorkers() {
		return numberOfWorkers;
	}

	public void setNumberOfWorkers(int numberOfWorkers) {
		this.numberOfWorkers = numberOfWorkers;
	}

	public int getNumberOfSpouts() {
		return numberOfSpouts;
	}

	public void setNumberOfSpouts(int numberOfSpouts) {
		this.numberOfSpouts = numberOfSpouts;
	}

	public int getNumberOfSplitterBolts() {
		return numberOfSplitterBolts;
	}

	public void setNumberOfSplitterBolts(int numberOfSplitterBolts) {
		this.numberOfSplitterBolts = numberOfSplitterBolts;
	}

	public int getNumberOfWorkerBolts() {
		return numberOfWorkerBolts;
	}

	public void setNumberOfWorkerBolts(int numberOfWorkerBolts) {
		this.numberOfWorkerBolts = numberOfWorkerBolts;
	}

	public int getNumberOfAggregatorBolts() {
		return numberOfAggregatorBolts;
	}

	public void setNumberOfAggregatorBolts(int numberOfAggregatorBolts) {
		this.numberOfAggregatorBolts = numberOfAggregatorBolts;
	}

	public int getNumberOfResultBolts() {
		return numberOfResultBolts;
	}

	public void setNumberOfResultBolts(int numberOfResultBolts) {
		this.numberOfResultBolts = numberOfResultBolts;
	}

	public int getNumberOfTasks() {
		return numberOfTasks;
	}

	public void setNumberOfTasks(int numberOfTasks) {
		this.numberOfTasks = numberOfTasks;
	}

	public long getTimeIntervalOfWorkerBolts() {
		return timeIntervalOfWorkerBolts;
	}

	public void setTimeIntervalOfWorkerBolts(long timeIntervalOfWorkerBolts) {
		this.timeIntervalOfWorkerBolts = timeIntervalOfWorkerBolts;
	}

	public long getTimeIntervalOfAggregatorBolts() {
		return timeIntervalOfAggregatorBolts;
	}

	public void setTimeIntervalOfAggregatorBolts(long timeIntervalOfAggregatorBolts) {
		this.timeIntervalOfAggregatorBolts = timeIntervalOfAggregatorBolts;
	}

	public long getTerminationTimeout() {
		return terminationTimeout;
	}

	public void setTerminationTimeout(long terminationTimeout) {
		this.terminationTimeout = terminationTimeout;
	}

	public long getTopologyTimeout() {
		return topologyTimeout;
	}

	public void setTopologyTimeout(long topologyTimeout) {
		this.topologyTimeout = topologyTimeout;
	}

	public long getTimeIntervalOfDataStreams() {
		return timeIntervalOfDataStreams;
	}

	public void setTimeIntervalOfDataStreams(long timeIntervalOfDataStreams) {
		this.timeIntervalOfDataStreams = timeIntervalOfDataStreams;
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public GroupingType getGroupingType() {
		return groupingType;
	}

	public void setGroupingType(GroupingType groupingType) {
		this.groupingType = groupingType;
	}

	public AggregatorType getAggregatorType() {
		return aggregatorType;
	}

	public void setAggregatorType(AggregatorType aggregatorType) {
		this.aggregatorType = aggregatorType;
	}

	public long getProcessDuration() {
		return processDuration;
	}

	public void setProcessDuration(long processDuration) {
		this.processDuration = processDuration;
	}

	public long getAggregationDuration() {
		return aggregationDuration;
	}

	public void setAggregationDuration(long aggregationDuration) {
		this.aggregationDuration = aggregationDuration;
	}

	public boolean isLogEnabled() {
		return isLogEnabled;
	}

	public void setLogEnabled(boolean logEnabled) {
		isLogEnabled = logEnabled;
	}
}
