package com.orhundalabasmaz.storm.loadbalancer.bolts;

import com.orhundalabasmaz.storm.model.Message;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

/**
 * @author Orhun Dalabasmaz
 */
public class KafkaOutputBolt extends KafkaBolt<String, Message> {

	public KafkaOutputBolt(String sinkName) {
		this(sinkName, "key", "message");
	}

	@SuppressWarnings("unchecked")
	public KafkaOutputBolt(String sinkName, String key, String message) {
		withTopicSelector(new DefaultTopicSelector(sinkName));
		withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, Message>(key, message));
	}

}
