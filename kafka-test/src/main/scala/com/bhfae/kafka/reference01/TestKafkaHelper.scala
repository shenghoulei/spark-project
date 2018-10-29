package com.bhfae.kafka.reference01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestKafkaHelper {

	def main(args: Array[String]): Unit = {

		// 0 设定日志的输出级别
		Logger.getLogger("org").setLevel(Level.WARN)

		//		if (args.length < 5) {
		//			println("Usage:<timeInterval> <brokerList> <zkQuorum> <topic> <group>")
		//			System.exit(1)
		//		}

		//		val Array(timeInterval, brokerList, zkQuorum, topic, group) = args
		// 注意：topic、group名必须一致且已经存在
		val timeInterval = "10"
		val brokerList = "hadoop03:9092,hadoop04:9092,hadoop05:9092"
		val zkQuorum = "hadoop01:2181,hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181"
		val topic: String = "ssjt_test"
		val group = "ssjt_test"

		val conf = new SparkConf().setAppName("KafkaDirectStream").setMaster("local[5]")
		val ssc = new StreamingContext(conf, Seconds(timeInterval.toInt))

		//kafka配置参数
		val kafkaParams = Map(
			"metadata.broker.list" -> brokerList,
			"group.id" -> group,
			"auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
		)

		val kafkaHelper = new KafkaHelper(kafkaParams, zkQuorum, topic, group)

		val kafkaStream = kafkaHelper.createDirectStream(ssc)

		var offsetRanges = Array[OffsetRange]()
		kafkaStream.transform(rdd => {
			offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			rdd
		}).map(msg => msg._2)
				.foreachRDD(rdd => {
					rdd.foreachPartition(partition => {
						partition.foreach(record => {
							//处理数据的方法
							println(record)
						})
					})
					kafkaHelper.updateZkOffsets(offsetRanges)
				})

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}
