package utils

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

class DateTool {


}

object DateTool {

	//1 获取今天的日期
	def getNowDate: String = {
		val now = Calendar.getInstance()
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		dateFormat.format(now.getTime)
	}

	//	2、获取昨天的日期
	def getYesterday: String = {
		val cal = Calendar.getInstance()
		cal.add(Calendar.DATE, -1)
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		dateFormat.format(cal.getTime)
	}

	//3、获取本周开始的日期
	def getNowWeekStart: String = {
		val cal = Calendar.getInstance()
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
		val df = new SimpleDateFormat("yyyy-MM-dd")
		df.format(cal.getTime)
	}

	//4、获取本周末的日期
	def getNowWeekEnd: String = {
		val cal = Calendar.getInstance()
		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY) //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
		cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
		val df = new SimpleDateFormat("yyyy-MM-dd")
		df.format(cal.getTime)
	}

	//5、本月的第一天
	def getNowMonthStart: String = {
		val cal: Calendar = Calendar.getInstance()
		cal.set(Calendar.DATE, 1)
		val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		df.format(cal.getTime) //本月第一天
	}

	//6、本月的最后一天
	def getNowMonthEnd: String = {
		val cal: Calendar = Calendar.getInstance()
		cal.set(Calendar.DATE, 1)
		cal.roll(Calendar.DATE, -1)
		val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		df.format(cal.getTime)
	}

	//7、将时间戳转化成日期,时间戳是秒数， 需要乘以1000l转化成毫秒
	def DateFormat(time: String): String = {
		val sdf = new SimpleDateFormat("yyyy-MM-dd")
		val date = sdf.format(new Date(time.toLong * 1000l))
		date
	}

	//8、时间戳转化为时间，原理同上
	def timeFormat(time: String): String = {
		val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
		val date: String = sdf.format(new Date(time.toLong * 1000l))
		date
	}

	//10 计算时间差
	def getCoreTime(start_time: String, end_Time: String): String = {
		val df = new SimpleDateFormat("HH:mm:ss")
		val begin = df.parse(start_time)
		val end = df.parse(end_Time)
		val between = (end.getTime - begin.getTime) / 1000
		val hour = between.toFloat / 3600
		val decf = new DecimalFormat("#.00") //自定义输出格式
		decf.format(hour)
	}
}