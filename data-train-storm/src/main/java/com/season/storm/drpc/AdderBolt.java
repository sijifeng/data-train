package com.season.storm.drpc;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.security.InvalidParameterException;

/**
 * Created by jiyc on 2017/6/13.
 */
public class AdderBolt extends BaseBasicBolt {
	private OutputCollector collector;

	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("输入参数的个数：" + input.size());
		String[] numbers = input.getString(1).split("\\+");
		Integer added = 0;

		try {
			if (numbers.length < 2) {
				throw new InvalidParameterException(
						"Should be at least 2 numbers");
			}
			for (String num : numbers) {
				added += Integer.parseInt(num);
			}
		} catch (Exception e) {
			collector.emit(new Values(input.getValue(0), "NULL"));
		}
		collector.emit(new Values(input.getValue(0), added));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

}
