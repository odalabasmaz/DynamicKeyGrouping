package com.orhundalabasmaz.storm.config;

import com.orhundalabasmaz.storm.common.SourceType;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;

import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class ConfigurationBuilder {
	private Configuration conf;
	private long additionalDuration = 10_000;

	private ConfigurationBuilder() {
		conf = new Configuration();
	}

	public static ConfigurationBuilder getInstance() {
		return new ConfigurationBuilder();
	}

	public ConfigurationBuilder defaultSet() {
		conf.setLogEnabled(true);
		conf.setNumberOfWorkers(1);
		conf.setNumberOfTasks(2);
		conf.setNumberOfSplitterBolts(3);
		conf.setNumberOfAggregatorBolts(1);
		conf.setNumberOfOutputBolts(1);
		conf.setTimeIntervalOfDataStreams(1);
		conf.setTimeIntervalOfWorkerBolts(2);
		conf.setTimeIntervalOfAggregatorBolts(5);
		conf.setTimeIntervalOfCheck(10_000);
		return this;
	}

	public ConfigurationBuilder appVersion(String appVersion) {
		conf.setAppVersion(appVersion);
		return this;
	}

	public ConfigurationBuilder testId(String testId) {
		conf.setTestId(testId);
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

	public ConfigurationBuilder numberOfOutputBolts(int numberOfOutputBolts) {
		conf.setNumberOfOutputBolts(numberOfOutputBolts);
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
		conf.setTopologyTimeout(terminationDuration + additionalDuration);
		return this;
	}

	public ConfigurationBuilder topologyTimeout(long topologyTimeout) {
		conf.setTopologyTimeout(topologyTimeout);
		return this;
	}

	public ConfigurationBuilder sourceType(SourceType sourceType) {
		conf.setSourceType(sourceType);
		return this;
	}

	public ConfigurationBuilder groupingType(GroupingType groupingType) {
		conf.setGroupingType(groupingType);
		return this;
	}

	public ConfigurationBuilder groupingProps(Map<String, String> props) {
		conf.addGroupingProps(props);
		return this;
	}

	public ConfigurationBuilder processDuration(long processDuration) {
		conf.setProcessDuration(processDuration);
		return this;
	}

	public ConfigurationBuilder countCycle(int countCycle) {
		conf.setCountCycle(countCycle);
		return this;
	}

	public ConfigurationBuilder enableLogging(boolean isLogEnabled) {
		conf.setLogEnabled(isLogEnabled);
		return this;
	}

	public ConfigurationBuilder stormMode(StormMode stormMode) {
		conf.setStormMode(stormMode);
		return this;
	}

	public ConfigurationBuilder sourceName(String sourceName) {
		conf.setSourceName(sourceName);
		return this;
	}

	public ConfigurationBuilder serverIp(String serverIp) {
		conf.setServerIp(serverIp);
		return this;
	}

	public Configuration build() {
		return conf;
	}
}
