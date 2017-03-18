package com.orhundalabasmaz.storm.data.message;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewByLangMessage extends Message {
	private String lang;
	private String page;
	private int n;
	private int m;

	public WikipediaPageviewByLangMessage(String lang, String page, int n, int m) {
		this.lang = lang;
		this.page = page;
		this.n = n;
		this.m = m;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	public int getM() {
		return m;
	}

	public void setM(int m) {
		this.m = m;
	}
}
