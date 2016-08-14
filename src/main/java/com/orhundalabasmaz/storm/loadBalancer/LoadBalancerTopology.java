package com.orhundalabasmaz.storm.loadBalancer;

import com.orhundalabasmaz.storm.common.ITopology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorBolt;
import com.orhundalabasmaz.storm.loadBalancer.bolts.WorkerBolt;
import com.orhundalabasmaz.storm.loadBalancer.bolts.OutputResultsBolt;
import com.orhundalabasmaz.storm.loadBalancer.bolts.SplitterBolt;
import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.DynamicKeyGrouping;
import com.orhundalabasmaz.storm.loadBalancer.grouping.*;
import com.orhundalabasmaz.storm.loadBalancer.spouts.CountrySpout;
import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import static com.orhundalabasmaz.storm.loadBalancer.Configuration.*;

/**
 * @author Orhun Dalabasmaz
 */
public class LoadBalancerTopology implements ITopology {
	private final String topologyName = "load-balancer-topology";
	private final String spoutName = "load-balancer-spout";
	private final String splitterBoltName = "splitter-bolt";
	private final String workerBoltName = "worker-bolt";
	private final String aggregatorBoltName = "aggregator-bolt";
	private final String resultBoltName = "result-bolt";
	private final String dataKey = "counts";
	private final String resultKey = "result";

	private Config conf;
	private StormTopology topology;
	private final StormMode mode;
	private final GroupingType groupingType = Configuration.GROUPING_TYPE;

	public LoadBalancerTopology(StormMode mode) {
		this.mode = mode;
	}

	private void logInitialConfig() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n")
				.append("Initializing LoadBalancerTopology!").append("\n")
				.append("APP VERSION: ").append(Configuration.APP_VERSION).append("\n")
				.append("DATA TYPE: ").append(Configuration.DATA_TYPE).append("\n")
				.append("SPLITTER: ").append(Configuration.GROUPING_TYPE).append("\n")
				.append("AGGREGATOR: ").append(Configuration.AGGREGATOR_TYPE).append("\n")
				.append("NUMBER OF WORKERS: ").append(Configuration.N_WORKER_BOLTS).append("\n")
				.append("RUNTIME DURATION: ").append(Configuration.TOPOLOGY_TIMEOUT / 60000).append(" min").append("\n")
				.append("STORM MODE: ").append(mode).append("\n")
				.append("==================================");
		Logger.log(sb.toString());
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
	 *     inputs may be numbers rather than country names 100 - 100_000_000
	 * 11. we'll be using real-world data
	 * <p>
	 * RESULT: should be at least 2 target bolt for beginning and expand the whole..
	 */
	@Override
	public void init() {
		// logs runtime configuration
		logInitialConfig();

		conf = new Config();
		conf.setNumWorkers(N_WORKERS);
		conf.setDebug(false);
//		conf.setMaxSpoutPending(1);
//		conf.setMaxTaskParallelism(16);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(spoutName, new CountrySpout(), N_SPOUTS);   //parallelism hint as number of executor

		// splitter
		builder.setBolt(splitterBoltName, new SplitterBolt(), N_SPLITTER_BOLTS)
//				.setNumTasks(2)
				.shuffleGrouping(spoutName);

		// counter
		BoltDeclarer counterDeclarer = builder.setBolt(workerBoltName, new WorkerBolt(T_WORKER_BOLTS), N_WORKER_BOLTS);
		switch (groupingType) {
			case SHUFFLE:
//				counterDeclarer.shuffleGrouping(splitterBoltName);
				counterDeclarer.customGrouping(splitterBoltName, new ShuffleGrouping());
				break;
			case KEY:
//				counterDeclarer.fieldsGrouping(splitterBoltName, new Fields(dataKey));
				counterDeclarer.customGrouping(splitterBoltName, new KeyGrouping());
				break;
			case PARTIAL_KEY:
				counterDeclarer.customGrouping(splitterBoltName, new PartialKeyGrouping());
				break;
			case DYNAMIC_KEY:
				counterDeclarer.customGrouping(splitterBoltName, new DynamicKeyGrouping());
				break;
			default:
				throw new UnsupportedOperationException("Unexpected groupingType: " + groupingType);
		}

		// aggregator
		builder.setBolt(aggregatorBoltName, new AggregatorBolt(T_AGGREGATOR_BOLTS), N_AGGREGATOR_BOLTS)
				.fieldsGrouping(workerBoltName, new Fields("boltId", dataKey));

		// result
		builder.setBolt(resultBoltName, new OutputResultsBolt(), N_RESULT_BOLTS)
				.fieldsGrouping(aggregatorBoltName, new Fields(resultKey));

		topology = builder.createTopology();
	}

	@Override
	public void run() {
		switch (mode) {
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

		Utils.sleep(TOPOLOGY_TIMEOUT);

		Logger.log("topology# killing topology");
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	private void runOnCluster() {
		try {
			Logger.log("topology# submitting topology on cluster");
//            StormSubmitter.submitTopology(topologyName, conf, topology);
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
	}
}
