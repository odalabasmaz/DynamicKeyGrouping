package com.orhundalabasmaz.storm.data.message;

/**
 * @author Orhun Dalabasmaz
 */
public class WikipediaPageviewsByLangMessage extends Message {
	private String lang;
	private String page;
	private long n;
	private long m;

	public WikipediaPageviewsByLangMessage(String lang, String page, long n, long m) {
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

	public long getN() {
		return n;
	}

	public void setN(long n) {
		this.n = n;
	}

	public long getM() {
		return m;
	}

	public void setM(long m) {
		this.m = m;
	}
}
