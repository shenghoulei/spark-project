package ssjt

import org.apache.spark.sql.SparkSession

object ReadMysql {

	// 1 SparkSession
	val spark: SparkSession = SparkSession
			.builder()
			.appName("Spark SQL basic example")
			.master("local[5]")
			.config("spark.some.config.option", "some-value")
			.getOrCreate()


	def main(args: Array[String]): Unit = {

		// 2 read mysql
		val df = spark
				.read
				.format("jdbc")
				.option("url", "jdbc:mysql://10.10.200.110:3306/ssjt_test")
				.option("user", "root")
				.option("password", "root")
				.option("dbtable", "people")
				.load()

		// 3 Looks the schema of this DataFrame.
		df.printSchema()

		// 4 Counts people by age
		val countsByAge = df.groupBy("age").count()
		countsByAge.show()

		// 5 Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("people")
		val sqlDF = spark.sql("SELECT * FROM people")
		sqlDF.show()

		// 6 Saving data to a JDBC source
		sqlDF.write
				.format("jdbc")
				.option("url", "jdbc:mysql://10.10.200.110:3306/ssjt_test")
				.option("user", "root")
				.option("password", "root")
				.option("dbtable", "people2")
				.save()

		// Saves countsByAge to S3 in the JSON format.
		//		countsByAge.write.format("json").save("s3a://...")
		spark.stop()
	}


}
