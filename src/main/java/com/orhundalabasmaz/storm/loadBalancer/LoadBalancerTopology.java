package com.orhundalabasmaz.storm.loadBalancer;

import com.orhundalabasmaz.storm.common.ITopology;
import com.orhundalabasmaz.storm.common.StormMode;
import com.orhundalabasmaz.storm.loadBalancer.bolts.AggregatorBolt;
import com.orhundalabasmaz.storm.loadBalancer.bolts.CounterBolt;
import com.orhundalabasmaz.storm.loadBalancer.bolts.OutputResultsBolt;
import com.orhundalabasmaz.storm.loadBalancer.bolts.SplitterBolt;
import com.orhundalabasmaz.storm.loadBalancer.grouping.PartialKeyGrouping;
import com.orhundalabasmaz.storm.loadBalancer.grouping.DynamicKeyGrouping;
import com.orhundalabasmaz.storm.loadBalancer.spouts.CountrySpout;
import com.orhundalabasmaz.storm.utils.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
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
	private final String counterBoltName = "counter-bolt";
	private final String aggregatorBoltName = "aggregator-bolt";
	private final String resultBoltName = "result-bolt";

	private Config conf;
	private StormTopology topology;
	private StormMode mode = StormMode.LOCAL;

	public LoadBalancerTopology(StormMode mode) {
		this.mode = mode;
	}

	/**
	 * TODO LIST
	 * 1. convert task to objects rather than primitives
	 * 2. should be included to SAMOA? (should use storm-core 0.9.4)
	 * 3. Use Log4j instead of Logger
	 * <p>
	 * RESULT: should be at least 2 target bolt for beginning and expand the whole..
	 */
	@Override
	public void init() {
		conf = new Config();
		conf.setNumWorkers(N_WORKERS);
		conf.setDebug(false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(spoutName, new CountrySpout(), N_SPOUTS);   //parallelism hint as number of executor

		// splitter
		builder.setBolt(splitterBoltName, new SplitterBolt(), N_SPLITTER_BOLTS)
//				.setNumTasks(2)
				.shuffleGrouping(spoutName);

		// counter
		builder.setBolt(counterBoltName, new CounterBolt(T_COUNTER_BOLTS), N_COUNTER_BOLTS)
//				.shuffleGrouping(splitterBoltName);
//				.fieldsGrouping(splitterBoltName, new Fields("country"));
//				.customGrouping(splitterBoltName, new PartialKeyGrouping());
				.customGrouping(splitterBoltName, new DynamicKeyGrouping());

		// aggregator
		builder.setBolt(aggregatorBoltName, new AggregatorBolt(T_AGGREGATOR_BOLTS), N_AGGREGATOR_BOLTS)
				.fieldsGrouping(counterBoltName, new Fields("country"));

		// result
		builder.setBolt(resultBoltName, new OutputResultsBolt(), N_RESULT_BOLTS)
				.fieldsGrouping(aggregatorBoltName, new Fields("result"));

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
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}
	}
}
