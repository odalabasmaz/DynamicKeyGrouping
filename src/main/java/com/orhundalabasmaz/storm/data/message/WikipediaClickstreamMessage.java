package com.orhundalabasmaz.storm.data.message;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaClickstreamMessage extends Message {
	private String prev;
	private String curr;
	private String type;
	private int n;

	public WikipediaClickstreamMessage(String prev, String curr, String type, int n) {
		this.prev = prev;
		this.curr = curr;
		this.type = type;
		this.n = n;
	}

	public String getPrev() {
		return prev;
	}

	public void setPrev(String prev) {
		this.prev = prev;
	}

	public String getCurr() {
		return curr;
	}

	public void setCurr(String curr) {
		this.curr = curr;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}
}
