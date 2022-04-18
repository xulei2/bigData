//package com.xl
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object Spark_wordCount extends Test{
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
//    val context = new SparkContext(sparkConf)
//    val lines:RDD[String] = context.textFile("datas")
//    val words:RDD[String] = lines.flatMap(_.split(" "))
//    val group:RDD[(String ,Iterable[String])] = words.groupBy(words => words)
//    val wordAndConut = group.map {
//      case (word, list) => {
//        (word, list.size)
//      }
//    }
//    val tuples = wordAndConut.collect()
//    tuples.foreach(println)
//    context.stop()
//    get()
//  }
//}
//class Test{
//  def get():Unit={
//    println("sssssss")
//  }
//}
