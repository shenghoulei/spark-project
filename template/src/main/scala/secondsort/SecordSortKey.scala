package secondsort

class SecordSortKey(firstKey: Int, secondKey: Int) extends Ordered[SecordSortKey] with Serializable {
	private val first:Int=firstKey
	private val second:Int=secondKey

	// 指定排序规则
	override def compare(that: SecordSortKey): Int = {
		// 第一排序规则
		if (this.first != that.first) {
			return this.first - that.first
		}
		// 第二排序规则
		this.second - that.second
	}

}

