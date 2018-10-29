package ssjt


import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadJson {
	def main(args: Array[String]) {
		// 1 定义conf
		val conf = new SparkConf().setMaster("local[5]").setAppName("mySpark")

		// 2 定义SparkContext
		val sc = new SparkContext(conf)

		val spark: SparkSession = SparkSession
				.builder()
				.config("spark.some.config.option", "some-value")
				.getOrCreate()

		// 3 ReadJson
		val frame = spark.read.json("D:\\data\\people.txt")
		frame.printSchema()
		frame.createGlobalTempView("people")
		val peoples = spark.sql("select * from global_temp.people")
		peoples.show()


	}

}

