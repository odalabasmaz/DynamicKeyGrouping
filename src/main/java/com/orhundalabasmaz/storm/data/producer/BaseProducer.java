package com.orhundalabasmaz.storm.data.producer;

import com.orhundalabasmaz.storm.data.serializer.JsonSerializer;
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

	private Producer producer;
	private final String filePath;
	private final String topicName;
	//	private final String servers = "85.110.34.250:9092";
//	private final String servers = "localhost:9092";
	private final String servers = "192.168.1.39:9092";

	protected BaseProducer(String filePath, String topicName) {
		this.filePath = filePath;
		this.topicName = topicName;
		init();
	}

	private void init() {
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//		configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 64);
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
			Map<String, Integer> map = new HashMap<>();
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
			LOGGER.info("Total emitted record in file {} is: {}", rootFile.getName(), totalCount);
			printResult(map);
		} catch (IOException e) {
			LOGGER.error("Exception occurred.", e);
		}
		producer.close();
		LOGGER.info("Producing done ...");
	}

	private long readFile(Map<String, Integer> map, Path path) throws IOException {
		long count = 0;
		String fileName = path.getFileName().toString();
		try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			String line;
			while ((line = br.readLine()) != null) {
				produce(map, line, fileName);
				++count;
			}
		}
		LOGGER.info("Total emitted record in file {} is: {}", fileName, count);
		return count;
	}

	protected abstract void produce(Map<String, Integer> map, String line, String fileName);

	@SuppressWarnings("unchecked")
	protected final void sendMessage(Object message) {
		ProducerRecord<String, String> rec = new ProducerRecord(topicName, message);
		producer.send(rec);
	}

	private void printResult(Map<String, Integer> map) {
		long count = 0;
		long begin = System.currentTimeMillis();
		StringBuilder result = new StringBuilder("key,count");
		TreeMap<String, Integer> treeMap = new TreeMap<>(map);
		for (Map.Entry<String, Integer> entry : treeMap.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();
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
