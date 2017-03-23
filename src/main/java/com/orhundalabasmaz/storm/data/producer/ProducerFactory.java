package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.common.SourceType;

/**
 * @author Orhun Dalabasmaz
 */
public class ProducerFactory {

	private ProducerFactory() {
	}

	public static ProducerFactory getInstance() {
		return new ProducerFactory();
	}

	public StreamProducer getStreamProducer(SourceType source, String filePath, String topicName) {
		StreamProducer producer;
		switch (source) {
			case TWITTER_TICKER:
				producer = new TwitterTickerProducer(filePath, topicName);
				break;
			case WIKIPEDIA_PAGEVIEWS:
				producer = new WikipediaPageviewProducer(filePath, topicName);
				break;
			case COUNTRY_SKEW:
			case COUNTRY_HALF_SKEW:
			case COUNTRY_BALANCED:
				producer = new CountryProducer(filePath, topicName);
				break;
			case TWITTER_ELECTION:
				producer = new TwitterElectionProducer(filePath, topicName);
				break;
			case WIKIPEDIA_CLICKSTREAM:
				producer = new WikipediaClickstreamProducer(filePath, topicName);
				break;
			case WIKIPEDIA_PAGEVIEWS_BY_LANG:
				producer = new WikipediaPageviewByLangProducer(filePath, topicName);
				break;
			default:
				throw new UnsupportedOperationException("SourceType not found! " + source.name());
		}
		return producer;
	}
}
