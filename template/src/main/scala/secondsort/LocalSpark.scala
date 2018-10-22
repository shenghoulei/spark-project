package secondsort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object LocalSpark {
	def main(args: Array[String]) {
		Logger.getLogger("org").setLevel(Level.WARN)
		// 1 定义conf
		val conf = new SparkConf().setMaster("local[5]").setAppName("mySpark")

		// 2 定义SparkContext
		val sc = new SparkContext(conf)


		// 3 读取源数据
		//		val lines = sc.wholeTextFiles("D://data/inverted/*", 2)
		val list = List((1, 2), (1, 1), (2, 2), (2, 1), (0, 2), (-1, 2))
		val lines = sc.makeRDD(list)
		val pairSortKey = lines.map { line =>
			(
					new SecordSortKey(line._1,line._2),
					line
			)
		}
		//第三步：使用sortByKey 基于自定义的key进行二次排序
		val sortPair = pairSortKey.sortByKey(false);

		val sortResult = sortPair.map(line => line._2);

		sortResult.collect().foreach { x => println(x) };


	}

}
