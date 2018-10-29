package com.sparkstreaming.main

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author litaoxiao
  *
  */
object ConsumerMain extends Serializable {

	def functionToCreateContext(): StreamingContext = {
		val sparkConf = new SparkConf().setAppName("WordFreqConsumer")
//				.setMaster("local[5]")
				.set("spark.local.dir", "/tmp")
				.set("spark.streaming.kafka.maxRatePerPartition", "10")
		val ssc = new StreamingContext(sparkConf, Seconds(10))

		// Create direct kafka stream with brokers and topics
		val brokerList = "hadoop03:9092,hadoop04:9092,hadoop05:9092"
		val topicsSet = "ssjt_test".split(",").toSet
		val group = "kafka_test"
		val autoOffsetReset = "smallest"
		val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> brokerList, "auto.offset.reset" -> autoOffsetReset, "group.id" -> group)
		val km = new KafkaManager(kafkaParams)
		val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

		// 缓存数据
		kafkaDirectStream.cache

		//do something......
		var offsetRanges = Array[OffsetRange]()
		val res = kafkaDirectStream.transform(rdd => {
			offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			println("- - - - - - - - - - - - - - - - - - ")
			rdd
		}).map(msg => msg._2)

		val res2: Unit = res.saveAsTextFiles("hdfs://hadoop05/kafka/" + new Date().getTime)



		//				.foreachRDD(rdd => {
		//					rdd.foreachPartition(partition => {
		//						partition.foreach(record => {
		//							//处理数据的方法
		//							println(record)
		//						})
		//					})
		//				})

		//更新zk中的offset
		kafkaDirectStream.foreachRDD(rdd => {
			if (!rdd.isEmpty) {
				km.updateZKOffsets(rdd)
			}
		})
		ssc
	}


	def main(args: Array[String]) {

		val ssc = functionToCreateContext()

		// Start the computation
		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}


}