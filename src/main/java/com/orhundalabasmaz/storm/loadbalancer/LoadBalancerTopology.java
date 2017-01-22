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
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.common.StreamingGroupFactory;
import com.orhundalabasmaz.storm.common.Topology;
import com.orhundalabasmaz.storm.config.Configuration;
import com.orhundalabasmaz.storm.loadbalancer.bolts.*;
import com.orhundalabasmaz.storm.loadbalancer.bolts.monitor.DigestBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.monitor.TotalKeyCountBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.monitor.WorkerDistributionBolt;
import com.orhundalabasmaz.storm.loadbalancer.bolts.output.KafkaOutputBolt;
import com.orhundalabasmaz.storm.utils.DKGUtils;
import com.orhundalabasmaz.storm.utils.Logger;
import storm.kafka.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * @author Orhun Dalabasmaz
 */
public class LoadBalancerTopology implements Topology {
	private final String topologyName = "dkg-topology";
	private final String spoutName = "load-balancer-spout";
	private final String splitterBoltName = "splitter-bolt";
	private final String workerBoltName = "worker-bolt";
	private final String workerObserverBoltName = "worker-observer-bolt";
	private final String digestBoltName = "digest-bolt";
	private final String workerDistributionBoltName = "worker-distribution-bolt";
	private final String totalKeyCountBoltName = "total-key-count-bolt";
	private final String aggregatorBoltName = "aggregator-bolt";
	private final String outputBoltName = "output-bolt";
	private final String dataKey = "counts";
	private final String resultKey = "result";

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
				.append("SPLITTER: ").append(runtimeConf.getGroupingType()).append("\n")
				.append("AGGREGATOR: ").append(runtimeConf.getAggregatorType()).append("\n")
				.append("NUMBER OF WORKER BOLTS: ").append(runtimeConf.getNumberOfWorkerBolts()).append("\n")
				.append("RUNTIME DURATION: ").append(runtimeConf.getTopologyTimeout() / 60000).append(" min").append("\n")
				.append("STORM MODE: ").append(stormMode).append("\n")
				.append("==================================");
		Logger.log(sb.toString());
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
	 * 3. use Log4j instead of Logger
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

		// stream grouping
		CustomStreamGrouping streamGrouping = StreamingGroupFactory
				.getInstance()
				.getStreamGrouping(runtimeConf.getGroupingType());

		//todo: parallelism hint must match with the number of kafka partitions
		builder.setSpout(spoutName, new KafkaSpout(getKafkaSpoutConfig()), runtimeConf.getNumberOfSpouts());
		//builder.setSpout(spoutName, new CountrySpout(runtimeConf.getStreamType()), runtimeConf.getNumberOfSpouts());   //parallelism hint as number of executor

		// splitter
		builder.setBolt(splitterBoltName,
				new SplitterBolt(), runtimeConf.getNumberOfSplitterBolts())
//				.setNumTasks(2)
				.shuffleGrouping(spoutName);

		// worker
		builder.setBolt(workerBoltName,
				new WorkerBolt(runtimeConf.getTimeIntervalOfWorkerBolts(), runtimeConf.getProcessDuration(), runtimeConf.getAggregationDuration()), runtimeConf.getNumberOfWorkerBolts())
				.customGrouping(splitterBoltName, streamGrouping);

		// aggregator
		builder.setBolt(aggregatorBoltName,
				new AggregatorBolt(runtimeConf.getTimeIntervalOfAggregatorBolts(), runtimeConf.getAggregationDuration()), 1)
				.fieldsGrouping(workerBoltName, new Fields("workerId", "counts"));

		// total key count
		builder.setBolt(totalKeyCountBoltName,
				new TotalKeyCountBolt(), 1)
				.noneGrouping(aggregatorBoltName);

		// key digest
		builder.setBolt(digestBoltName,
				new DigestBolt(), 1)
				.noneGrouping(aggregatorBoltName);

		// work distribution
		builder.setBolt(workerDistributionBoltName,
				new WorkerDistributionBolt(), 1)
				.noneGrouping(aggregatorBoltName);

		// workers
		builder.setBolt(workerObserverBoltName,
				new WorkerObserverBolt(), 10)
				.fieldsGrouping(workerBoltName, new Fields("workerId"));

		// output
		builder.setBolt(outputBoltName + "-1",
				new KafkaOutputBolt("total"), 1)
				.shuffleGrouping(totalKeyCountBoltName);
		builder.setBolt(outputBoltName + "-2",
				new KafkaOutputBolt("digest"), 1)
				.shuffleGrouping(digestBoltName);
		builder.setBolt(outputBoltName + "-3",
				new KafkaOutputBolt("worker"), 1)
				.shuffleGrouping(workerDistributionBoltName);
		builder.setBolt(outputBoltName + "-4",
				new KafkaOutputBolt("workers"), 1)
				.shuffleGrouping(workerObserverBoltName);

		// result
//		builder.setBolt(outputBoltName, new OutputResultsBolt(), runtimeConf.getNumberOfOutputBolts())
//				.fieldsGrouping(aggregatorBoltName, new Fields(resultKey));

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
		Logger.log("topology# submitting topology on local");
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);

		DKGUtils.sleepInMilliseconds(runtimeConf.getTopologyTimeout());

		Logger.log("topology# killing topology");
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	private void runOnCluster() {
		try {
			Logger.log("topology# submitting topology on cluster");
//            StormSubmitter.submitTopology(topologyName, conf, topology);
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			e.printStackTrace();
		}
	}
}
