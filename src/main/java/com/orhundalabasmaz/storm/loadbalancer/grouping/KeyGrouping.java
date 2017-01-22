package com.orhundalabasmaz.storm.loadbalancer.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * default key grouping
 */
public class KeyGrouping implements CustomStreamGrouping, Serializable {
	private static final long serialVersionUID = 4347587663034269976L;
	private List<Integer> targetTasks;
	private HashFunction h1 = Hashing.murmur3_128(13);

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<>(1);
		if (!values.isEmpty()) {
			String key = values.get(0).toString(); // assume key is the first field
			int selected = (int) (Math.abs(h1.hashBytes(key.getBytes()).asLong()) % this.targetTasks.size());
			boltIds.add(targetTasks.get(selected));
		}
		return boltIds;
	}
}