package ssjt.read

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReadFile {
	def main(args: Array[String]) {

		//这里指在本地运行，2个线程，一个监听，一个处理数据
		val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[5]")

		// Create the context
		val ssc = new StreamingContext(sparkConf, Seconds(10)) // 时间划分为10秒

		val lines = ssc.textFileStream("hdfs://hadoop05/test")
		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

		wordCounts.foreachRDD(rdd=>{
			rdd.collect().foreach(println)
			println()
		})

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}
