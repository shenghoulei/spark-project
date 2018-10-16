import java.sql.{Connection, DriverManager, PreparedStatement}

class WriteMysql extends Serializable {

	def myFun(iterator: Iterator[(String, Long)]): Unit = {
		var conn: Connection = null
		var ps: PreparedStatement = null
		val sql = "insert into test(date, count) values (?, ?)"
		try {
			conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/spark", "root", "root")
			iterator.foreach(data => {
				ps = conn.prepareStatement(sql)
				ps.setString(1, data._1)
				ps.setLong(2, data._2)
				ps.executeUpdate()
			}
			)
		} catch {
			case _: Exception => println("Mysql Exception")
		} finally {
			if (ps != null) {
				ps.close()
			}
			if (conn != null) {
				conn.close()
			}
		}
	}

}
