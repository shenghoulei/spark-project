package bhfae

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
		val lines = sc.wholeTextFiles("D://data/inverted/*", 2)
		val list = List(((1, 2), "shy"), ((1, 0), "shy"), ((2, 1), "shy"), ((2, 0), "shy"))
		val rdd = sc.makeRDD(list)
		val res = rdd.sortByKey().collect()
		res.foreach(println)
		sc.stop()


	}

}
