package com.orhundalabasmaz.storm.model;

/**
 * @author Orhun Dalabasmaz
 */
public class WorkersMessage extends Message {
	private String workerId;
	private Long count;

	public WorkersMessage(String key) {
		super(key);
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}
}
