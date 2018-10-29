package ssjt

import org.apache.spark.sql.{Encoder, SparkSession}

object ReadText {

	case class Person(name: String, age: Long)

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

		// 3 读取方式-1(dateset)
		// Encoders are created for case classes
		val caseClassDS = Seq(Person("Andy", 32)).toDS()
		caseClassDS.createOrReplaceTempView("people_1")
		spark.sql("select * from people_1").show()

		// Encoders for most common types are automatically provided by importing spark.implicits._
		val primitiveDS = Seq(1, 2, 3).toDS()
		val unit = primitiveDS.map(_ + 1)
		unit.rdd.foreach(println)
		// Returns: Array(2, 3, 4)

		// 4 读取方式-2
		// Create an RDD of Person objects from a text file, convert it to a Dataframe
		val peopleDF = spark.sparkContext
				.textFile("examples/src/main/resources/people.txt")
				.map(_.split(","))
				.map(attributes => Person(attributes(0), attributes(1).trim.toInt))
				.toDF()
		// Register the DataFrame as a temporary view
		peopleDF.createOrReplaceTempView("people")

		// SQL statements can be run by using the sql methods provided by Spark
		val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

		// The columns of a row in the result can be accessed by field index
		teenagersDF.map(teenager => "Name: " + teenager(0)).show()
		// +------------+
		// |       value|
		// +------------+
		// |Name: Justin|
		// +------------+

		// or by field name
		teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
		// +------------+
		// |       value|
		// +------------+
		// |Name: Justin|
		// +------------+

		// No pre-defined encoders for Dataset[Map[K,V]], define explicitly
		implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
		// Primitive types and case classes can be also defined as
		// implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

		// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
		teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
		// Array(Map("name" -> "Justin", "age" -> 19))

	}
}