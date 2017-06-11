package com.season.storm.bolt;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.season.storm.handler.logInfoHandler;
import com.season.storm.model.ordersBean;

/**
 * Created by jiyc on 2017/6/11.
 */
public class merchantsSalesAnalysisBolt {
    private OutputCollector _collector;
    logInfoHandler loginfohandler;
    //JedisPool pool;

    public void execute(Tuple tuple) {
        String orderInfo = tuple.getString(0);
        ordersBean order = loginfohandler.getOrdersBean(orderInfo);

        //store the salesByMerchant infomation into Redis
        //Jedis jedis = pool.getResource();
        //jedis.zincrby("orderAna:topSalesByMerchant", order.getTotalPrice(), order.getMerchantName());
    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this._collector = collector;
        this.loginfohandler = new logInfoHandler();
        //this.pool = new JedisPool(new JedisPoolConfig(), "ymhHadoop",6379,2 * 60000,"12345");

    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }

}
