package com.orhundalabasmaz.storm.common;

/**
 * @author Orhun Dalabasmaz
 */
public enum SourceType {
	TWITTER_TICKER("twitter-ticker"), WIKIPEDIA_PAGEVIEWS("wikipedia-pageviews"),
	COUNTRY_SKEW_R0("country-skew-r0"), COUNTRY_SKEW_R10("country-skew-r10"), COUNTRY_SKEW_R20("country-skew-r20"), COUNTRY_SKEW_R30("country-skew-r30"),
	COUNTRY_SKEW_R40("country-skew-r40"), COUNTRY_SKEW_R50("country-skew-r50"), COUNTRY_SKEW_R60("country-skew-r60"), COUNTRY_SKEW_R70("country-skew-r70"),
	COUNTRY_SKEW_R80("country-skew-r80"), COUNTRY_SKEW_R90("country-skew-r90"), COUNTRY_SKEW_R100("country-skew-r100"), COUNTRY_HALF_SKEW_R80("country-half-skew-r80"),
	TWITTER_ELECTION("twitter-election"), WIKIPEDIA_CLICKSTREAM("wikipedia-clickstream"), WIKIPEDIA_PAGEVIEWS_BY_LANG("wikipedia-pageviews-by-lang");

	private String key;

	SourceType(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public static String[] keys() {
		SourceType[] values = SourceType.values();
		String[] keys = new String[values.length];
		int i = 0;
		for (SourceType value : values) {
			keys[i++] = value.getKey();
		}
		return keys;
	}
}
