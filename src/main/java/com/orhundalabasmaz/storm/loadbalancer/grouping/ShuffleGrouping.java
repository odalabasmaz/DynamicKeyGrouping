package com.orhundalabasmaz.storm.loadbalancer.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * default shuffle grouping
 */
public class ShuffleGrouping implements CustomStreamGrouping, Serializable {
	private static final long serialVersionUID = -8251986623724271906L;
	private int index;
	private List<Integer> targetTasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.index = -1;
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<>(1);
		setNextIndex();
		boltIds.add(targetTasks.get(index));
		return boltIds;
	}

	private void setNextIndex() {
		index = ++index % targetTasks.size();
	}
}