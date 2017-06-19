package com.season.streaming.scala.kafka


import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiyc on 2017/6/12.
  */
object StreamingKafka /*extends Logging */ {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Kafka Sample1")

    //https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    //http://www.ne.jp/asahi/hishidama/home/tech/scala/spark/SparkContext.html
    conf.setMaster("local[*]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")

    /*    val sc = new SparkContext(conf)
        setStreamingLogLevels*/
    // https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$
    val ssc = new StreamingContext(conf, Seconds(10))

    // Default Groupid
    // http://kafka.apache.org/07/configuration.html
    // https://spark.apache.org/docs/1.3.0/streaming-kafka-integration.html
    // OSX: home brew
    //      cat /usr/local/etc/kafka/consumer.properties
    //      group.id=test-consumer-group

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.78.49:9092",
      "group.id" -> "streaming_kafka_test",
      "auto.offset.reset" -> "smallest",
      "socket.receive.buffer.bytes" -> "1024000000")
    val topics = Set("test")

    val km = new KafkaManager(kafkaParams)

    val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder, Tuple4[String, Int, Long, String]](
      ssc, kafkaParams, topics, (mmd: MessageAndMetadata[String, String]) => {
        (mmd.topic, mmd.partition, mmd.offset, mmd.message().toString)
      }
    )

    kafkaStream.foreachRDD(
      rdd =>{
        rdd.foreach(println)
      }
    )

    // KafkaUtils.createDirectStream()
    //val topicLines = KafkaUtils.createStream(ssc, "192.168.78.49:2181", "test-consumer-group", topics)

    //topicLines.print()

    ssc.start()

    ssc.awaitTermination()

  }


  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      /* logInfo("Setting log level to [WARN] for streaming example." +
         " To override add a custom log4j.properties to the classpath.")*/
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

}
