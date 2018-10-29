package ssjt

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ReadTextSchema {
	def main(args: Array[String]): Unit = {
		// 1 sparkSession
		val spark: SparkSession = SparkSession
				.builder()
				.config("spark.some.config.option", "some-value")
				.appName(s"${this.getClass.getSimpleName}")
				.master("local[5]")
				.getOrCreate()

		// 2 必须要引入的
		import spark.implicits._

		// 3 Create an RDD
		val peopleRDD = spark.sparkContext.textFile("d:\\data\\people.txt")

		// 4 Generate the schema based on the string of schema
		val schema = StructType(
			StructField("id", IntegerType,nullable = true)
					:: StructField("name", StringType, nullable = true)
					:: Nil)

		// 5 Convert records of the RDD (people) to Rows
		// Apply the schema to the RDD
		val rowRDD = peopleRDD
				.map(_.split(" "))
				.map(attributes => Row(attributes(0).toInt, attributes(1).trim))
		val peopleDF = spark.createDataFrame(rowRDD, schema)

		// 6 Creates a temporary view using the DataFrame
		peopleDF.createOrReplaceTempView("people")

		// 7 SQL can be run over a temporary view created using DataFrames
		val results = spark.sql("SELECT * FROM people")
		results.show()

		// 8 The results of SQL queries are DataFrames and support all the normal RDD operations
		// The columns of a row in the result can be accessed by field index or by field name
		results.map(attributes => "Name: " + attributes(0)).show()

	}

}
