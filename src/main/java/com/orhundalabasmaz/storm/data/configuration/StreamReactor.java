package com.orhundalabasmaz.storm.data.configuration;

import com.orhundalabasmaz.storm.common.SourceType;
import com.orhundalabasmaz.storm.loadbalancer.grouping.GroupingType;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Orhun Dalabasmaz
 */
public class StreamReactor {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamReactor.class);

	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private static final String[] dataTypes = SourceType.keys();

	private static final List<String> spouts = Arrays.asList("5"); //, "10", "15", "20"

	private static final List<String> workers = Arrays.asList("5", "10", "50", "100");

	private static final String[] algos = GroupingType.keys();

	private static final List<String> observers = Arrays.asList("aggregator", "distribution", "splitter", "worker");

	private static final String outputDir = "D:\\cloud\\stream-reactor\\generated\\";

	private static final String fileNameTemplate = "%s-%s-s%s-w%s-%s-%s-sink";

	private static final String NL = "\n";

	public void generateScripts() {
		createScriptDirectory();
		generateStartScript();
		generateStopScript();
		generateRunnerScript();
	}

	private void createScriptDirectory() {
		try {
			Files.createDirectory(Paths.get(outputDir + "scripts"));
		} catch (IOException e) {
			LOGGER.error("Could not create directory!", e);
		}
	}

	private void generateStartScript() {
		StringBuilder builder = new StringBuilder()
				.append("#!/bin/bash").append(NL).append(NL)
				.append("DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"").append(NL).append(NL);

		for (String dataType : dataTypes) {
			builder.append("# ").append(dataType).append(NL);
			for (String spout : spouts) {
				for (String worker : workers) {
					for (String algo : algos) {
						for (String observer : observers) {
							String fileName = String.format(fileNameTemplate, dataType, spout, spout, worker, algo, observer);
							builder.append("$DIR/cli.sh create ").append(fileName)
									.append(" < ").append("conf/").append(dataType).append("/").append(fileName).append(".properties")
									.append(NL);
						}
					}
				}
			}
			builder.append(NL);
		}

		writeToFile(builder.toString(), outputDir + "scripts\\start-all.sh");
	}

	private void generateStopScript() {
		StringBuilder builder = new StringBuilder()
				.append("#!/bin/bash").append(NL).append(NL)
				.append("DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"").append(NL).append(NL);

		for (String dataType : dataTypes) {
			builder.append("# ").append(dataType).append(NL);
			for (String spout : spouts) {
				for (String worker : workers) {
					for (String algo : algos) {
						for (String observer : observers) {
							String fileName = String.format(fileNameTemplate, dataType, spout, spout, worker, algo, observer);
							builder.append("$DIR/cli.sh rm ").append(fileName).append(NL);
						}
					}
				}
			}
			builder.append(NL);
		}

		writeToFile(builder.toString(), outputDir + "scripts\\stop-all.sh");
	}

	private void generateRunnerScript() {
		StringBuilder builder = new StringBuilder();

		for (SourceType dataType : SourceType.values()) {
			for (String spout : spouts) {
				for (String worker : workers) {
					for (GroupingType algo : GroupingType.values()) {
						builder.append("nohup java -jar dkg-wd.jar LOCAL ").append(dataType).append(" ")
								.append(algo).append(" ").append(dataType.getKey()).append("-").append(spout).append(" ")
								.append(spout).append(" ").append(worker)
								.append(" >> ").append("/home/logs/dkg/").append(dataType.getKey()).append("-").append(spout).append("-")
								.append("s").append(spout).append("-").append("w").append(worker).append("-").append(algo.getType())
								.append(".out &").append(NL);
					}
				}
			}
			builder.append(NL);
		}

		writeToFile(builder.toString(), outputDir + "scripts\\runner.sh");
	}

	private void writeToFile(String line, String filePath) {
		try {
			Path path = Files.createFile(Paths.get(filePath));
			Files.write(path, line.getBytes(CHARSET));
		} catch (IOException e) {
			LOGGER.error("File could not be generated!", e);
		}
	}

	public void generateFiles() {
		for (String dataType : dataTypes) {
			for (String spout : spouts) {
				for (String worker : workers) {
					for (String algo : algos) {
						for (String observer : observers) {
							try {
								generateFile(dataType, spout, worker, algo, observer);
							} catch (IOException e) {
								LOGGER.error("File could not be generated!", e);
							}
						}
					}
				}
			}
		}
	}

	private void generateFile(String dataType, String spout, String worker, String algo, String observer) throws IOException {
		String fullNameTemplate = fileNameTemplate + ".properties";
		String fileName = String.format(fullNameTemplate, dataType, spout, spout, worker, algo, observer);
		File resource = new File(getClass().getClassLoader().getResource("template-sink.properties").getFile());
		File target = new File(outputDir + "conf" + "\\" + dataType + "\\" + fileName);

		FileUtils.copyFile(resource, target);

		Map<String, String> replacementMap = new HashMap<>();
		replacementMap.put("<DATA_TYPE>", dataType + "-" + spout);
		replacementMap.put("<DATABASE>", dataType);
		replacementMap.put("<SPOUT>", spout);
		replacementMap.put("<WORKER>", worker);
		replacementMap.put("<ALGO>", algo);
		replacementMap.put("<OBSERVER>", observer);
		replace(target, replacementMap);
	}

	private void replace(File file, Map<String, String> replacementMap) throws IOException {
		Path path = Paths.get(file.toURI());
		String content = new String(Files.readAllBytes(path), CHARSET);
		for (Map.Entry<String, String> entry : replacementMap.entrySet()) {
			content = content.replaceAll(entry.getKey(), entry.getValue());
		}
		Files.write(path, content.getBytes(CHARSET));
	}
}
