package com.orhundalabasmaz.storm.config;

import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.StreamType;

/**
 * @author Orhun Dalabasmaz
 */
public class ConfigurationBuilder {
	private Configuration conf;

	public ConfigurationBuilder() {
		conf = new Configuration();
	}

	public void defaultSet() {
		conf.setLogEnabled(true);
		conf.setNumberOfWorkers(1);
		conf.setNumberOfTasks(2);
		conf.setNumberOfSplitterBolts(3);
		conf.setNumberOfAggregatorBolts(1);
		conf.setNumberOfResultBolts(1);
		conf.setTimeIntervalOfDataStreams(1);
		conf.setTimeIntervalOfWorkerBolts(5);
		conf.setTimeIntervalOfAggregatorBolts(15);
		conf.setTimeIntervalOfCheck(10_000);
		conf.setAggregationDuration(0);
		conf.setAggregatorType(AggregatorType.CUMULATIVE);

	}

	public ConfigurationBuilder appVersion(String appVersion) {
		conf.setAppVersion(appVersion);
		return this;
	}

	public ConfigurationBuilder testId(String testId) {
		conf.setTestId(testId);
		return this;
	}

	public ConfigurationBuilder dataSet(String dataSet) {
		conf.setDataSet(dataSet);
		return this;
	}

	public ConfigurationBuilder numberOfWorkers(int numberOfWorkers) {
		conf.setNumberOfWorkers(numberOfWorkers);
		return this;
	}

	public ConfigurationBuilder numberOfSpouts(int numberOfSpouts) {
		conf.setNumberOfSpouts(numberOfSpouts);
		return this;
	}

	public ConfigurationBuilder numberOfSplitterBolts(int numberOfSplitterBolts) {
		conf.setNumberOfSplitterBolts(numberOfSplitterBolts);
		return this;
	}

	public ConfigurationBuilder numberOfWorkerBolts(int numberOfWorkerBolts) {
		conf.setNumberOfWorkerBolts(numberOfWorkerBolts);
		return this;
	}

	public ConfigurationBuilder numberOfAggregatorBolts(int numberOfAggregatorBolts) {
		conf.setNumberOfAggregatorBolts(numberOfAggregatorBolts);
		return this;
	}

	public ConfigurationBuilder numberOfResultBolts(int numberOfResultBolts) {
		conf.setNumberOfResultBolts(numberOfResultBolts);
		return this;
	}

	public ConfigurationBuilder numberOfTasks(int numberOfTasks) {
		conf.setNumberOfTasks(numberOfTasks);
		return this;
	}

	public ConfigurationBuilder timeIntervalOfDataStreams(long timeIntervalOfDataStreams) {
		conf.setTimeIntervalOfDataStreams(timeIntervalOfDataStreams);
		return this;
	}

	public ConfigurationBuilder timeIntervalOfWorkerBolts(long timeIntervalOfWorkerBolts) {
		conf.setTimeIntervalOfWorkerBolts(timeIntervalOfWorkerBolts);
		return this;
	}

	public ConfigurationBuilder timeIntervalOfAggregatorBolts(long timeIntervalOfAggregatorBolts) {
		conf.setTimeIntervalOfAggregatorBolts(timeIntervalOfAggregatorBolts);
		return this;
	}

	public ConfigurationBuilder timeIntervalOfCheck(long timeIntervalOfCheck) {
		conf.setTimeIntervalOfCheck(timeIntervalOfCheck);
		return this;
	}

	public ConfigurationBuilder terminationDuration(long terminationDuration) {
		conf.setTerminationDuration(terminationDuration);
		return this;
	}

	public ConfigurationBuilder topologyTimeout(long topologyTimeout) {
		conf.setTopologyTimeout(topologyTimeout);
		return this;
	}

	public ConfigurationBuilder streamType(StreamType streamType) {
		conf.setStreamType(streamType);
		return this;
	}

	public ConfigurationBuilder groupingType(GroupingType groupingType) {
		conf.setGroupingType(groupingType);
		return this;
	}

	public ConfigurationBuilder aggregatorType(AggregatorType aggregatorType) {
		conf.setAggregatorType(aggregatorType);
		return this;
	}

	public ConfigurationBuilder processDuration(long processDuration) {
		conf.setProcessDuration(processDuration);
		return this;
	}

	public ConfigurationBuilder aggregationDuration(long aggregationDuration) {
		conf.setAggregationDuration(aggregationDuration);
		return this;
	}

	public ConfigurationBuilder enableLogging(boolean isLogEnabled) {
		conf.setLogEnabled(isLogEnabled);
		return this;
	}

	public Configuration build() {
		return conf;
	}
}
