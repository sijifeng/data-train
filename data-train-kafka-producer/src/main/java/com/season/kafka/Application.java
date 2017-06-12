package com.season.kafka;

import com.season.kafka.config.KafkaProperties;
import com.season.kafka.consumer.Consumer;
import com.season.kafka.prducer.Producer;

/**
 * Created by jiyc on 2017/6/11.
 */
public class Application {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
        producerThread.start();

     /*   Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
        consumerThread.start();*/

    }
}
