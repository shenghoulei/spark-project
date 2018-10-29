package ssjt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StopGracefullyHdfs_1 {

	def main(args: Array[String]) {
		// 1 定义conf
		val conf = new SparkConf().setMaster("local[5]").setAppName("mySpark")

		// 2 定义SparkContext
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc, Seconds(3))

		val ds = ssc.socketTextStream("hadoop02", 9999)
		ds.print()

		ssc.start()
		stopByMarkFile(ssc)
		ssc.awaitTermination()
		ssc.stop()
	}

	/**
	  * 根据是否存在停止应用的消息文件，决定是否停止应用
	  *
	  * @param ssc
	  */
	def stopByMarkFile(ssc: StreamingContext): Unit = {
		val timeOut = 5 * 1000 //Wait for the execution to stop.设置超时时间
		var isStop = false // 循环检测的标记
		val path = "hdfs://hadoop05:8020/test/stop" //判断是否停止应用的消息文件

		while (!isStop) {
			// 检测应用是否停止
			isStop = ssc.awaitTerminationOrTimeout(timeOut)

			// 应用没有停止且已经收到停止应用的消息
			if (!isStop && isExistsMarkFile(path)) {
				println("现在开始关闭程序---------")
				ssc.stop(stopSparkContext = true, stopGracefully = true)
				println("关闭程序成功---------")
			}

			// 此次没有收到停止应用的消息
			if (!isExistsMarkFile(path)) {
				print("没有检测到关闭信号")
				// 设置检测的间隔
				Thread.sleep(10000)
			}
		}
	}

	/**
	  * 判断hdfs上是否存在停止应用的消息文件夹
	  *
	  * @param path 要检测的消息文件夹
	  * @return 存在要检测的消息文件夹返回true，不存在返回false
	  */
	def isExistsMarkFile(path: String): Boolean = {
		val fs = FileSystem.get(new Configuration())
		fs.exists(new Path(path))
	}

}