package com.orhundalabasmaz.storm.common;

import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.splitter.*;

/**
 * @author Orhun Dalabasmaz
 */
public class SourceFactory {

	private SourceFactory() {
	}

	public static SourceFactory getInstance() {
		return new SourceFactory();
	}

	public SplitterBolt getSourceSplitter(SourceType sourceType) {
		SplitterBolt splitter;
		switch (sourceType) {
			case TWITTER_TICKER:
				splitter = new TwitterTickerSplitter();
				break;
			case WIKIPEDIA_PAGEVIEWS:
				splitter = new WikipediaPageviewsSplitter();
				break;
			case COUNTRY_SKEW_R0:
			case COUNTRY_SKEW_R10:
			case COUNTRY_SKEW_R20:
			case COUNTRY_SKEW_R30:
			case COUNTRY_SKEW_R40:
			case COUNTRY_SKEW_R50:
			case COUNTRY_SKEW_R60:
			case COUNTRY_SKEW_R70:
			case COUNTRY_SKEW_R80:
			case COUNTRY_SKEW_R90:
			case COUNTRY_SKEW_R100:
			case COUNTRY_HALF_SKEW_R80:
				splitter = new CountrySplitter();
				break;
			case TWITTER_ELECTION:
				splitter = new TwitterElectionSplitter();
				break;
			case WIKIPEDIA_CLICKSTREAM:
				splitter = new WikipediaClickstreamSplitter();
				break;
			case WIKIPEDIA_PAGEVIEWS_BY_LANG:
				splitter = new WikipediaPageviewsByLangSplitter();
				break;
			default:
				throw new UnsupportedOperationException("Unexpected sourceType: " + sourceType);
		}
		return splitter;
	}
}
