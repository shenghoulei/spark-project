package ssjt

import java.util.Date

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StopGracefullyHdfs {
	// 1 定义conf
	val conf: SparkConf = new SparkConf()
			//			.setMaster("local[5]")
			.setAppName("mySpark")

	// 2 定义SparkContext
	val sc = new SparkContext(conf)
	val ssc = new StreamingContext(sc, Seconds(5))

	def main(args: Array[String]) {

		val ds = ssc.socketTextStream("hadoop03", 8888)
		ds.saveAsTextFiles("hdfs://hadoop05:8020/result/stop" + new Date().getTime)
		//		ds.print()

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
				println("没有检测到关闭信号")
				// 设置检测的间隔
				Thread.sleep(6000)
			}
		}
	}

	/**
	  * 判断hdfs上是否存在停止应用的消息文件夹,用异常来处理
	  *
	  * @param path 要检测的消息文件夹
	  * @return 存在要检测的消息文件夹返回true，不存在返回false
	  */
	def isExistsMarkFile(path: String): Boolean = {
		try {
			val fs = sc.textFile("hdfs://hadoop05/test/stop/stop-1")
			val flag = fs.take(1)
			true
		} catch {
			case ex: Exception => false
		}
	}

}