package bhfae


object Common {

	def main(args: Array[String]) {

		val v2=2

		val res=v2 match {
			case 1=> 1
			case 2=> 2
			case _=> 3
		}

		println(res)

		def  f(str:String)="f("+str+")"
		def  g(str:String)="g("+str+")"

		val fg=f _ compose g
		println(fg("success"))


	}





}
