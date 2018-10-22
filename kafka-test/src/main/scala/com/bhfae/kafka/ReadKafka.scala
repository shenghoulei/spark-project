package com.bhfae.kafka

import java.util.Date

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReadKafka {

	def main(args: Array[String]): Unit = {

		// 1 本地环境设置---至少启动两个,一个监听,一个消费
		val conf = new SparkConf().setAppName("ReadKafka")

		// 2 SparkContext
		val sc = new SparkContext(conf)

		// 3 StreamingContext
		val ssc = new StreamingContext(sc, Seconds(10))
		//		ssc.checkpoint("d://check1801")

		// 4 设置连接连接到zookeeper的集群
		val zkHosts = "hadoop01:2181,hadoop02:2181,hadoop03:2181,,hadoop04:2181,hadoop05:2181"

		// 5 设置kafka的组名和主题
		val groupName = "gp1"

		// map的key是主题名,value是消费的线程数.也可以消费多个主题
		val topic = Map("ssjt_test" -> 1)

		// 6 从kafka读取数据(从kafka的偏移量存到zookeeper),并输出
		val kafkaStream = KafkaUtils.createStream(ssc, zkHosts, groupName, topic).map(_._2)
		val result = kafkaStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
		val date=new Date().getTime.toString
		result.saveAsTextFiles("hdfs://hadoop05/kafka/"+date)


		// 7 启动SparkStreaming
		ssc.start()
		// 8 保持SparkStreaming线程一直开启
		ssc.awaitTermination()

	}

}
