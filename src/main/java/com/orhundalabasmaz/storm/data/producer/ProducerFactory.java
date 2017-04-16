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
				producer = new WikipediaPageviewsProducer(filePath, topicName);
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
				producer = new CountryProducer(filePath, topicName);
				break;
			case TWITTER_ELECTION:
				producer = new TwitterElectionProducer(filePath, topicName);
				break;
			case WIKIPEDIA_CLICKSTREAM:
				producer = new WikipediaClickstreamProducer(filePath, topicName);
				break;
			case WIKIPEDIA_PAGEVIEWS_BY_LANG:
				producer = new WikipediaPageviewsByLangProducer(filePath, topicName);
				break;
			default:
				throw new UnsupportedOperationException("SourceType not found! " + source.name());
		}
		return producer;
	}
}
