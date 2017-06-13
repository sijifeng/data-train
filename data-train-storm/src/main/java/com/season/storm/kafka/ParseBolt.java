package com.season.storm.kafka;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by jiyc on 2017/6/13.
 */
public class ParseBolt extends BaseBasicBolt {
	public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
		String word = tuple.getString(0);
		System.out.println(word);
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
