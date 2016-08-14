package com.orhundalabasmaz.storm.config;

import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorType;
import com.orhundalabasmaz.storm.loadBalancer.grouping.GroupingType;
import com.orhundalabasmaz.storm.loadBalancer.spouts.DataType;

/**
 * @author Orhun Dalabasmaz
 */
public class ConfigurationBuilder {
	private Configuration conf;

	public ConfigurationBuilder() {
		conf = new Configuration();
	}

	public ConfigurationBuilder appVersion(String appVersion) {
		conf.setAppVersion(appVersion);
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

	public ConfigurationBuilder timeIntervalOfWorkerBolts(int timeIntervalOfWorkerBolts) {
		conf.setTimeIntervalOfWorkerBolts(timeIntervalOfWorkerBolts);
		return this;
	}

	public ConfigurationBuilder timeIntervalOfAggregatorBolts(int timeIntervalOfAggregatorBolts) {
		conf.setTimeIntervalOfAggregatorBolts(timeIntervalOfAggregatorBolts);
		return this;
	}

	public ConfigurationBuilder terminationTimeout(long terminationTimeout) {
		conf.setTerminationTimeout(terminationTimeout);
		return this;
	}

	public ConfigurationBuilder topologyTimeout(int topologyTimeout) {
		conf.setTopologyTimeout(topologyTimeout);
		return this;
	}

	public ConfigurationBuilder timeIntervalOfDataStreams(long timeIntervalOfDataStreams) {
		conf.setTimeIntervalOfDataStreams(timeIntervalOfDataStreams);
		return this;
	}

	public ConfigurationBuilder dataType(DataType dataType) {
		conf.setDataType(dataType);
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

	public ConfigurationBuilder setLogEnabled(boolean isLogEnabled) {
		conf.setLogEnabled(isLogEnabled);
		return this;
	}

	public Configuration build() {
		return conf;
	}
}
