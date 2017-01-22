package com.orhundalabasmaz.storm.config;

import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.loadbalancer.bolts.old.AggregatorType;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadbalancer.spouts.StreamType;

import java.io.Serializable;

/**
 * @author Orhun Dalabasmaz
 */
public class Configuration implements Serializable {
	// APP VERSION
	private String appVersion = "v1.0";
	private String testId;

	// NUMBER OF PROCESS UNITS
	private int numberOfWorkers;
	private int numberOfSpouts;
	private int numberOfSplitterBolts;
	private int numberOfWorkerBolts;
	private int numberOfAggregatorBolts;
	private int numberOfOutputBolts;
	private int numberOfTasks;

	// TIME INTERVAL
	private long timeIntervalOfDataStreams;         // ms (default 1 ms)
	private long timeIntervalOfWorkerBolts;         // sec
	private long timeIntervalOfAggregatorBolts;     // sec
	private long timeIntervalOfCheck;               // ms

	// TIMEOUTS
	private long terminationDuration;               // ms
	private long topologyTimeout;                   // ms

	// RUNTIME PROPS
	private String dataSet;
	private StreamType streamType;
	private GroupingType groupingType;
	private AggregatorType aggregatorType;
	private long processDuration;                   //ms
	private long aggregationDuration;               //ms
	private boolean isLogEnabled;
	private int retryCount;
	private StormMode stormMode;

	// KAFKA
	private String sourceName;
	private String sinkName;

	// SERVER
	private String IPAddr;

	/* getters & setters */
	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

	public String getTestId() {
		return testId;
	}

	public void setTestId(String testId) {
		this.testId = testId;
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

	public int getNumberOfOutputBolts() {
		return numberOfOutputBolts;
	}

	public void setNumberOfOutputBolts(int numberOfOutputBolts) {
		this.numberOfOutputBolts = numberOfOutputBolts;
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

	public long getTimeIntervalOfCheck() {
		return timeIntervalOfCheck;
	}

	public void setTimeIntervalOfCheck(long timeIntervalOfCheck) {
		this.timeIntervalOfCheck = timeIntervalOfCheck;
	}

	public long getTerminationDuration() {
		return terminationDuration;
	}

	public void setTerminationDuration(long terminationDuration) {
		this.terminationDuration = terminationDuration;
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

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public StormMode getStormMode() {
		return stormMode;
	}

	public void setStormMode(StormMode stormMode) {
		this.stormMode = stormMode;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getSinkName() {
		return sinkName;
	}

	public void setSinkName(String sinkName) {
		this.sinkName = sinkName;
	}

	public String getIPAddr() {
		return IPAddr;
	}

	public void setIPAddr(String IPAddr) {
		this.IPAddr = IPAddr;
	}
}
