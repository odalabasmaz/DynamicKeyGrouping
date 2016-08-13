package com.orhundalabasmaz.storm.loadBalancer.grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

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