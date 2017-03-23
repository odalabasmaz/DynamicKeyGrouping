package com.orhundalabasmaz.storm.common;

/**
 * @author Orhun Dalabasmaz
 */
public enum SourceType {
	TWITTER_TICKER("twitter-ticker"), WIKIPEDIA_PAGEVIEWS("wikipedia-pageview"),
	COUNTRY_SKEW("country-skew"), COUNTRY_HALF_SKEW("country-half-skew"), COUNTRY_BALANCED("country-balanced"),
	TWITTER_ELECTION("twitter-election"), WIKIPEDIA_CLICKSTREAM("wikipedia-pageviews"), WIKIPEDIA_PAGEVIEWS_BY_LANG("wikipedia-pageviews-by-lang");

	private String key;

	SourceType(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public static String[] keys() {
		return new String[]{TWITTER_TICKER.getKey(), WIKIPEDIA_PAGEVIEWS.getKey(),
				COUNTRY_SKEW.getKey(), COUNTRY_HALF_SKEW.getKey(), COUNTRY_BALANCED.getKey(),
				TWITTER_ELECTION.getKey(), WIKIPEDIA_CLICKSTREAM.getKey(), WIKIPEDIA_PAGEVIEWS_BY_LANG.getKey()};
	}
}
