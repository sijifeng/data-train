package com.season.streaming.scala.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jiyc on 2017/6/12.
  */
object StreamingKafka {
  def main(args: Array[String]): Unit = {

    // https://spark.apache.org/docs/latest/quick-start.html
    val conf = new SparkConf().setAppName("Spark Kafka Sample1")

    //https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    //http://www.ne.jp/asahi/hishidama/home/tech/scala/spark/SparkContext.html
    conf.setMaster("local[*]")

    // https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$
    val ssc = new StreamingContext(conf, Seconds(10))

    val topics = List(("test", 1)).toMap
    // Default Groupid
    // http://kafka.apache.org/07/configuration.html
    // https://spark.apache.org/docs/1.3.0/streaming-kafka-integration.html
    // OSX: home brew
    //      cat /usr/local/etc/kafka/consumer.properties
    //      group.id=test-consumer-group
    val topicLines = KafkaUtils.createStream(ssc, "192.168.78.49:2181", "test-consumer-group", topics)

    topicLines.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
