package com.orhundalabasmaz.storm.loadBalancer.grouping;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * default key grouping
 * */
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
		if (values.size() > 0) {
			String str = values.get(0).toString(); // assume key is the first field
			int selected = (int) (Math.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
			boltIds.add(targetTasks.get(selected));
		}
		return boltIds;
	}
}