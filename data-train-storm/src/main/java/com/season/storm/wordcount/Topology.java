package com.season.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by jiyc on 2017/6/11.
 */
public class Topology {
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
				.shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 2)
				.fieldsGrouping("word-normalizer", new Fields("word"));
		//配置
		Config conf = new Config();
		conf.put("wordsFile", "D:\\word.txt");
		conf.setDebug(false);
		//提交Topology
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);


		//提交远程
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3); // use three worker processes
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			//提交本地模式
			LocalCluster cluster = new LocalCluster();
			conf.setNumWorkers(3);
			cluster.submitTopology("localTopology", conf, builder.createTopology());

			Thread.sleep(10000);
			cluster.shutdown();
		}

	}
}
