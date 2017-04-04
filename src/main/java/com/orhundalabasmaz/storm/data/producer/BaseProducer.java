package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.message.Message;
import com.orhundalabasmaz.storm.data.serializer.JsonSerializer;
import com.orhundalabasmaz.storm.utils.DKGConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author Orhun Dalabasmaz
 */
public abstract class BaseProducer implements StreamProducer {
	private static Logger LOGGER = LoggerFactory.getLogger(BaseProducer.class);
	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private Producer producer;
	private final String filePath;
	private final String topicName;
	private final String servers = DKGConstants.SERVER_IP + ":9092";
	private final boolean enabled = false;

	protected BaseProducer(String filePath, String topicName) {
		this.filePath = filePath;
		this.topicName = topicName;
		init();
	}

	private void init() {
		/*
		* kafka producer configuration
		* for more information: https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
		* */
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProperties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
		configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 16);
		configProperties.put(ProducerConfig.ACKS_CONFIG, "0");
//		configProperties.put("producer.type", "async");
		/*props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);*/
		producer = new KafkaProducer(configProperties);
	}

	@Override
	public final void produceStream() {
		LOGGER.info("Producing stream ...");
		try {
			Map<String, Long> map = new HashMap<>();
			Path path = Paths.get(filePath);
			File rootFile = path.toFile();

			long totalCount = 0;
			boolean isDirectory = rootFile.isDirectory();
			if (isDirectory) {
				File[] files = rootFile.listFiles();
				assert files != null : "should be at least one file!";
				for (File file : files) {
					if (file.isDirectory()) {
						continue;
					}
					String absolutePath = file.getAbsolutePath();
					totalCount += readFile(map, Paths.get(absolutePath));
				}
			} else {
				totalCount += readFile(map, path);
			}
			LOGGER.info("Total emitted record in root file {} is: {} and total key count is: {}", rootFile.getName(), totalCount, map.get("TOTAL_COUNT"));
//			printResult(map);
		} catch (IOException e) {
			LOGGER.error("Exception occurred.", e);
		}
		producer.close();
		LOGGER.info("Producing done ...");
	}

	private long readFile(Map<String, Long> map, Path path) throws IOException {
		long count = 0;
		String fileName = path.getFileName().toString();
		try (BufferedReader br = Files.newBufferedReader(path, CHARSET)) {
			String line;
			while ((line = br.readLine()) != null) {
				produce(map, line, fileName);
				++count;
			}
		}
		LOGGER.info("Total emitted record in file {} is: {} and total key count is: {}", fileName, count, map.get("TOTAL_COUNT"));
		return count;
	}

	protected abstract void produce(Map<String, Long> map, String line, String fileName);

	@SuppressWarnings("unchecked")
	protected final void sendMessage(Message message) {
		if (enabled) {
			ProducerRecord<String, String> rec = new ProducerRecord(topicName, message);
			producer.send(rec);
		}
	}

	private void printResult(Map<String, Long> map) {
		long count = 0;
		long begin = System.currentTimeMillis();
		StringBuilder result = new StringBuilder("key,count");
		TreeMap<String, Long> treeMap = new TreeMap<>(map);
		for (Map.Entry<String, Long> entry : treeMap.entrySet()) {
			String key = entry.getKey();
			Long value = entry.getValue();
			count += value;
			result.append("\n").append(key).append(",").append(value);
		}
		long end = System.currentTimeMillis();
		long duration = (end - begin) / 1000;
		LOGGER.info("### EMITTED RECORDS ###\n{}", result);
		LOGGER.info(">>> Ordered in {} sec", duration);
		LOGGER.info(">>> Total produced: {}", count);
	}
}
