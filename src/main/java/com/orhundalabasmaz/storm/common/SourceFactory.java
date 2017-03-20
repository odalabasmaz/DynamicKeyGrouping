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
			case WIKIPEDIA_PAGEVIEW:
				splitter = new WikipediaPageviewSplitter();
				break;
			case COUNTRY:
				splitter = new CountrySplitter();
				break;
			case TWITTER_ELECTION:
				splitter = new TwitterElectionSplitter();
				break;
			case WIKIPEDIA_CLICKSTREAM:
				splitter = new WikipediaClickstreamSplitter();
				break;
			case WIKIPEDIA_PAGEVIEW_BY_LANG:
				splitter = new WikipediaPageviewByLangSplitter();
				break;
			default:
				throw new UnsupportedOperationException("Unexpected sourceType: " + sourceType);
		}
		return splitter;
	}
}