package bhfae

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {

	def main(args: Array[String]) {

		// 1 设置配置文件
		val conf = new SparkConf().setMaster("local[5]").setAppName("mySpark")

		// 2 获取spark core的应用上下文
		val sc = new SparkContext(conf)

		// 3 设定读取路径,读取数据
		val pathFile = "D:\\data\\test\\*"
		val texts: RDD[(String, String)] = sc.wholeTextFiles(pathFile)
		val lines: RDD[String] = texts.map(_._2)
		lines.toString()
		lines.collect().foreach(println)



	}

	/**
	  * 获取特定格式的当前日期字符串"2018-10-09"
	  *
	  * @return
	  */
	def getDateNow: String = {
		val now: Date = new Date()
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val dt = dateFormat.format(now)
		dt
	}


}
