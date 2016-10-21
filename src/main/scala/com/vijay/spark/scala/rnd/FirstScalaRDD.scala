
package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaSparkContext.toSparkContext
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

class FirstScalaRDD {
  val config = new SparkConf().setMaster("local").setAppName("FirstScalaRDD")
  val sc = new JavaSparkContext(config)

  def printWord(query: String) {
    val lines = sc.parallelize(List("panda", "I like panda", "do you like panda"))
    printRDD(lines)

    val likeRDD = lines.filter { x => lines.toString().contains("panda") }
    printRDD(likeRDD)

    val inputRDD1 = sc.textFile("log.txt")
   //val errorsRDD1 = inputRDD1.filter { line1 => line1.toString().contains("error") }

  }

  def printRDD(lines: RDD[String]) = {
    println(".............printRDD START...............")
    var words = lines.flatMap { line => line.split(" ") }
    for (word <- words)
      println(word)
    println(".............printRDD END  ...............")
  }

  def filterPanda(line: String): Boolean =
    {
      line.contains("panda")
    }

}