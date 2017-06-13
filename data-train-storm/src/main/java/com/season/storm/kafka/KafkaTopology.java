package com.season.storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jiyc on 2017/6/13.
 */
public class KafkaTopology {
	public static int NUM_WORKERS = 1;
	public static int NUM_ACKERS = 1;
	public static int MSG_TIMEOUT = 180;
	public static int SPOUT_PARALLELISM_HINT = 1;
	public static int PARSE_BOLT_PARALLELISM_HINT = 1;

	public StormTopology buildTopology(Map map) {
		String zkServer = map.get("zookeeper").toString();
		System.out.println("zkServer: " + zkServer);
		final BrokerHosts zkHosts = new ZkHosts(zkServer);
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "test", "/zkkafkaspout", "single-point-test");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		List<String> zkServers = new ArrayList<>();
		zkServers.add("192.168.78.49");
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), SPOUT_PARALLELISM_HINT);
		builder.setBolt("parseBolt", new ParseBolt(), PARSE_BOLT_PARALLELISM_HINT).shuffleGrouping("kafkaSpout");
		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("===========start===========");
		/*Map map = XmlHelper.Dom2Map("realtime.xml");*/
		Map map = new HashMap<>();
		map.put("zookeeper", "192.168.78.49:2181");

		KafkaTopology kafkaTopology = new KafkaTopology();
		StormTopology stormTopology = kafkaTopology.buildTopology(map);
		Config config = new Config();
		config.setNumWorkers(NUM_WORKERS);
		config.setNumAckers(NUM_ACKERS);
		config.setMessageTimeoutSecs(MSG_TIMEOUT);
		config.setMaxSpoutPending(5000);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("single-point-test", config, stormTopology);
		//StormSubmitter.submitTopology("single-point-test", config, stormTopology);
	}
}
