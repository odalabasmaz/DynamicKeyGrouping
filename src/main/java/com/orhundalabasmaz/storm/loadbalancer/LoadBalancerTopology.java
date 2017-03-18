package com.orhundalabasmaz.storm.loadbalancer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.orhundalabasmaz.storm.common.SourceFactory;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.common.StreamingGroupFactory;
import com.orhundalabasmaz.storm.common.Topology;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.loadbalancer.bolts.AggregatorBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.KafkaOutputBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.SplitterBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.WorkerBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.observer.DistributionObserverBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.observer.SplitterObserverBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.observer.WorkerObserverBolt;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * @author Orhun Dalabasmaz
 */
public class LoadBalancerTopology implements Topology {
	private final Logger LOGGER = LoggerFactory.getLogger(LoadBalancerTopology.class);
	private final String topologyName = "dkg-topology";
	private final String spoutName = "load-balancer-spout";
	private final String splitterName = "splitter";
	private final String workerName = "worker";
	private final String splitterObserverName = "splitter-observer";
	private final String workerObserverName = "worker-observer";
	private final String distributionObserverName = "key-distribution";
	private final String aggregatorName = "aggregator";
	private final String outputName = "output";

	private Config conf;
	private Configuration runtimeConf;
	private StormMode stormMode;
	private StormTopology topology;
	private boolean initialized = false;

	public LoadBalancerTopology(Configuration runtimeConf) {
		this.runtimeConf = runtimeConf;
		this.stormMode = runtimeConf.getStormMode();
	}

	private void logInitialConfig() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("Initializing LoadBalancerTopology!").append("\n")
				.append("APP VERSION: ").append(runtimeConf.getAppVersion()).append("\n")
				.append("STREAM TYPE: ").append(runtimeConf.getStreamType()).append("\n")
				.append("SOURCE TYPE: ").append(runtimeConf.getSourceType()).append("\n")
				.append("GROUPING TYPE: ").append(runtimeConf.getGroupingType()).append("\n")
				.append("NUMBER OF WORKER BOLTS: ").append(runtimeConf.getNumberOfWorkerBolts()).append("\n")
				.append("RUNTIME DURATION: ").append(runtimeConf.getTopologyTimeout() / 60000).append(" min").append("\n")
				.append("STORM MODE: ").append(stormMode).append("\n")
				.append("==================================");
		LOGGER.info(sb.toString());
	}

	private SpoutConfig getKafkaSpoutConfig() {
		String sourceName = runtimeConf.getSourceName();
		String zkConnString = runtimeConf.getIPAddr() + ":2181";
		BrokerHosts hosts = new ZkHosts(zkConnString);
		SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, sourceName, "/" + sourceName, UUID.randomUUID().toString());
		kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
		kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
		kafkaSpoutConfig.forceFromStart = true;
		kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		return kafkaSpoutConfig;
	}

	private void setKafkaProducerConfig(Config conf) {
		Map<String, String> kafkaProducerProps = new HashMap<>();
//		kafkaProducerProps.put("bootstrap.servers", IPADDR + ":9092");
		kafkaProducerProps.put("metadata.broker.list", runtimeConf.getIPAddr() + ":9092");
		kafkaProducerProps.put("request.required.acks", "1");    //todo ?
//		kafkaProducerProps.put("client.id", "");
//		kafkaProducerProps.put("batch.size", "16384");
//		kafkaProducerProps.put("retries", "1");
//		kafkaProducerProps.put("key.serializer", "kafka.serializer.StringEncoder");
//		kafkaProducerProps.put("value.serializer", "backtype.storm.multilang.JsonSerializer");
		kafkaProducerProps.put("key.serializer", "a");// "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducerProps.put("value.serializer", "b");//"backtype.storm.multilang.JsonSerializer");
		kafkaProducerProps.put("serializer.class", "com.orhundalabasmaz.storm.serializer.JsonEncoder");
//		kafkaProducerProps.put("deserializer.class", "com.orhundalabasmaz.storm.serializer.JsonDecoder");
//		kafkaProducerProps.put("serializer.class", "kafka.serializer.StringEncoder");
//		kafkaProducerProps.put("serializer.encoding", "UTF8");
		conf.put("kafka.broker.properties", kafkaProducerProps);
	}

	/**
	 * TODO LIST
	 * 1. convert task to objects rather than primitives
	 * 2. should be included to SAMOA? (should use storm-core 0.9.4)
	 * 3. use Log4j instead of CustomLogger
	 * 4. convert long to BigDecimal
	 * after a while long/int counts will reach the limits! (BigInteger)
	 * 5. min load percentage should be calculated with number of bolts
	 * 6. generic infrastructure for keys and models
	 * 7. use key as converted toLowerCase(key)
	 * 8. %25 -> %30
	 * 8. calc aggregator cost
	 * 9. to get new task must be more difficult when the target tasks reduced
	 * 10. input country list should vary between 10-1000 distinct values
	 * inputs may be numbers rather than country names 100 - 100_000_000
	 * 11. we'll be using real-world data
	 * <p>
	 * RESULT: should be at least 2 target bolt for beginning and expand the whole..
	 */
	@Override
	public void init() {
		// logs runtime configuration
		logInitialConfig();

		// create topology builder
		TopologyBuilder builder = new TopologyBuilder();

		// topology config
		conf = new Config();
		conf.setNumWorkers(runtimeConf.getNumberOfWorkers());
		conf.setDebug(false);
//		conf.setMaxSpoutPending(1);
//		conf.setMaxTaskParallelism(16);
		setKafkaProducerConfig(conf);

		// source
		SplitterBolt sourceSplitter = SourceFactory
				.getInstance()
				.getSourceSplitter(runtimeConf.getSourceType());

		// stream grouping
		CustomStreamGrouping streamGrouping = StreamingGroupFactory
				.getInstance()
				.getStreamGrouping(runtimeConf.getGroupingType(), runtimeConf.getGroupingProps());

		//todo: parallelism hint must match with the number of kafka partitions
		builder.setSpout(spoutName, new KafkaSpout(getKafkaSpoutConfig()), runtimeConf.getNumberOfSpouts());

		// splitter
		builder.setBolt(splitterName,
				sourceSplitter, runtimeConf.getNumberOfSplitterBolts())
//				.setNumTasks(2)
				.shuffleGrouping(spoutName);

		// worker
		builder.setBolt(workerName,
				new WorkerBolt(runtimeConf.getTimeIntervalOfWorkerBolts(), runtimeConf.getProcessDuration(),
						runtimeConf.getAggregationDuration()), runtimeConf.getNumberOfWorkerBolts())
				.customGrouping(splitterName, streamGrouping);

		// aggregator
		builder.setBolt(aggregatorName,
				new AggregatorBolt(runtimeConf.getTimeIntervalOfAggregatorBolts(), runtimeConf.getAggregationDuration()), 10)
				.fieldsGrouping(workerName, new Fields("key"));

		// splitter observer
		builder.setBolt(splitterObserverName,
				new SplitterObserverBolt(5), 1)
				.noneGrouping(splitterName);

		// worker observer
		builder.setBolt(workerObserverName,
				new WorkerObserverBolt(), 1)
				.noneGrouping(workerName);

		// distribution observer
		builder.setBolt(distributionObserverName,
				new DistributionObserverBolt(runtimeConf.getTimeIntervalOfAggregatorBolts()), 1)
				.noneGrouping(workerName);

		// output
		String sourceName = runtimeConf.getSourceName();
		String groupingType = runtimeConf.getGroupingType().getType();
		builder.setBolt(outputName + "-0",
				new KafkaOutputBolt(sourceName + "-" + groupingType + "-" + "splitter"), 1)
				.shuffleGrouping(splitterObserverName);
		builder.setBolt(outputName + "-1",
				new KafkaOutputBolt(sourceName + "-" + groupingType + "-" + "aggregator"), 1)
				.shuffleGrouping(aggregatorName);
		builder.setBolt(outputName + "-2",
				new KafkaOutputBolt(sourceName + "-" + groupingType + "-" + "worker"), 1)
				.shuffleGrouping(workerObserverName);
		builder.setBolt(outputName + "-3",
				new KafkaOutputBolt(sourceName + "-" + groupingType + "-" + "distribution"), 1)
				.shuffleGrouping(distributionObserverName);

		// result
//		builder.setBolt(outputName, new OutputResultsBolt(), runtimeConf.getNumberOfOutputBolts())
//				.fieldsGrouping(aggregatorName, new Fields(resultKey));

		topology = builder.createTopology();
		initialized = true;
	}

	@Override
	public void run() {
		if (!initialized) {
			throw new UnsupportedOperationException("init method should be called firstly.");
		}
		switch (stormMode) {
			case LOCAL:
				runOnLocal();
				break;
			case CLUSTER:
				runOnCluster();
				break;
			default:
				throw new UnsupportedOperationException("StormMode should be LOCAL or CLUSTER.");
		}
	}

	private void runOnLocal() {
		LOGGER.info("topology# submitting topology on local");
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);

		DKGUtils.sleepInMilliseconds(runtimeConf.getTopologyTimeout());

		LOGGER.info("topology# killing topology");
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	private void runOnCluster() {
		try {
			LOGGER.info("topology# submitting topology on cluster");
//            StormSubmitter.submitTopology(topologyName, conf, topology);
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			e.printStackTrace();
		}
	}
}
