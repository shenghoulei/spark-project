package bhfae

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object TestCount {

	def main(args: Array[String]) {

		// 1 设置配置文件
		val conf = new SparkConf().setAppName("mySpark")

		// 2 获取spark core的应用上下文
		val sc = new SparkContext(conf)

		// 3 设定读取路径,读取数据
		val pathFile = "hdfs://hadoop03/flume/"+ "word.txt"
		val texts = sc.textFile(pathFile)
		val date = "20181013"
		val total = texts.count()
		val count404 = texts.filter(_.contains("\" 404 ")).count()
		val ratio = count404 * 1.0 / total
		myFun(date, total, count404, ratio)
	}

	/**
	  * 10分钟为间隔
	  *
	  * @return
	  */
	def getFile: String = {
		val fm = new SimpleDateFormat("HH:mm")
		val tim = fm.format(new Date())
		val info = tim.split(":")
		val pre = info(0).toInt - 1
		val m = info(1).dropRight(1) + "0"
		pre + m
	}

	def myFun(data: (String, Long, Long, Double)): Unit = {
		var conn: Connection = null
		var ps: PreparedStatement = null
		val sql = "insert into test(date, total,count404,ratio) values (?, ?,?,?)"
		try {
			conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/spark", "root", "root")
			ps = conn.prepareStatement(sql)
			ps.setString(1, data._1)
			ps.setLong(2, data._2)
			ps.setLong(3, data._3)
			ps.setDouble(4, data._4)

			ps.executeUpdate()
		}  finally {
			if (ps != null) {
				ps.close()
			}
			if (conn != null) {
				conn.close()
			}
		}
	}
}
