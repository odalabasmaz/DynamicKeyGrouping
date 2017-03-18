package com.orhundalabasmaz.storm.data;

import com.orhundalabasmaz.storm.common.SourceType;
import com.orhundalabasmaz.storm.data.producer.ProducerFactory;
import com.orhundalabasmaz.storm.data.producer.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Orhun Dalabasmaz
 */
public class Producer {
	private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

	private Producer() {
	}

	/*
	* */
	public static void main(String[] args) {
		if (args.length != 3) {
			LOGGER.error("invalid params");
			throw new UnsupportedOperationException("invalid params");
		}

		SourceType sourceType = SourceType.valueOf(args[0]);
		String filePath = args[1];
		String topicName = args[2];

		StreamProducer producer = ProducerFactory.getInstance()
				.getStreamProducer(sourceType, filePath, topicName);

		long begin = System.currentTimeMillis();
		producer.produceStream();
		long end = System.currentTimeMillis();
		long duration = (end - begin) / 1000;
		LOGGER.info("Time consumed: {} sec", duration);
	}
}
