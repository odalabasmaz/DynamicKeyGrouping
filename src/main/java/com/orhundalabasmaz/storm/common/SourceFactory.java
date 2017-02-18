package com.orhundalabasmaz.storm.common;

import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.splitter.CountrySplitterBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.splitter.TwitterSplitterBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.splitter.WikipediaSplitterBolt;

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
			case COUNTRY:
				splitter = new CountrySplitterBolt();
				break;
			case WIKIPEDIA:
				splitter = new WikipediaSplitterBolt();
				break;
			case TWITTER:
				splitter = new TwitterSplitterBolt();
				break;
			default:
				throw new UnsupportedOperationException("Unexpected sourceType: " + sourceType);
		}
		return splitter;
	}
}
