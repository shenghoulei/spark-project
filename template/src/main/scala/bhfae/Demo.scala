package bhfae

import org.apache.spark.{SparkConf, SparkContext}
object Demo {
  def main(args: Array[String]) {
       val conf = new SparkConf().setAppName("mySpark")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    //conf.setMaster("local")
    val sc =new SparkContext(conf)
    val lines=sc.textFile("hdfs://hadoop03/user/hdfs/word.txt")
    val word=lines.flatMap(_.split(" ")).map((_,1))
    val wordCount=word.reduceByKey(_+_)
    wordCount.saveAsTextFile("hdfs://hadoop03/user/hdfs/result")

  }

}
