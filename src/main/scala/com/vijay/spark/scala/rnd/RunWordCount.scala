package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object RunWordCount {
  // base RDD
  val config = new SparkConf().setMaster("local").setAppName("RunWordCount")

  val sc = new JavaSparkContext(config)
  sc.setLogLevel("INFO")

  val lines = sc.textFile("E:\\Vijay\\Personnel\\RND\\Data\\catalina.out")
  
  def main(args: Array[String]): Unit = {
    println("Running Word Count...")
    
    var runWordCount = new WordCount(lines)

  }
}