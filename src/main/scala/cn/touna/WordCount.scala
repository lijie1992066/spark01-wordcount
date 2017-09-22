package cn.touna

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/18 
  * Time: 16:44  
  */
class WordCount {

}


object WordCount {
  def main(args: Array[String]): Unit = {
    var arg = Array("hdfs://lijie:9000/sz/access.log.fensi", "hdfs://lijie:9000/sz/out")
    //    val conf = new SparkConf().setMaster("spark://lijie:7077").setAppName("wd")
    val conf = new SparkConf().setMaster("local").setAppName("wd")
    val sc = new SparkContext(conf)
    println(1111)
    //    sc.textFile(arg(0)).flatMap(_.split(" ")).map((_, 1)).aggregateByKey(0)(_ + _, _ + _).sortBy(_._2, true).saveAsTextFile(arg(1))
    //    sc.textFile(arg(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, true).saveAsTextFile(arg(1))
    //    sc.textFile(arg(0)).flatMap(_.split(" ")).map((_,1)).combineByKey(x => x,(a:Int,b:Int) => a+b, (c:Int,d:Int) => c+d).sortBy(_._2,true).saveAsTextFile(arg(1))
    sc.textFile(arg(0)).flatMap(_.split(" ")).map((_, 1)).foldByKey(0)(_ + _).sortBy(_._2, true).saveAsTextFile(arg(1))
    println(2222)
    sc.stop
  }
}
