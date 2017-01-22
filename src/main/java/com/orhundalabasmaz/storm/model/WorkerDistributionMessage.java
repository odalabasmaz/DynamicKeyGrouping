package com.orhundalabasmaz.storm.model;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkerDistributionMessage extends Message {
	private String workerId;
	private Long totalCount;

	public WorkerDistributionMessage(String key, long timestamp) {
		super(key, timestamp);
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}
}
