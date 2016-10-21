package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD.toRDD
import org.apache.spark.api.java.JavaSparkContext
import java.io.Serializable

class TransformationRDD extends Serializable {
  val config = new SparkConf().setMaster("local").setAppName("TransformationRDD")
  val sc = new JavaSparkContext(config)
  sc.setLogLevel("FATAL")

  def parseFiles() {

    // base RDD
    val lines = sc.textFile("E:\\Vijay\\Personnel\\RND\\Data\\catalina.out")

    // transformed RDDs
    val errors = lines.filter((line: String) => line.contains("ERROR"))
   // val errors = lines.filter((line: String) => isMatch(line, "ERROR"))
    println("ErrorCount=" + errors.count())

    val exceptions = lines.filter((line: String) => line.contains("Exception"))
    println("ExceptionsCount=" + exceptions.count())

    val warnings = lines.filter((line: String) => line.contains("WARNING"))
    println("WarningsCount=" + warnings.count())

    val badlinesRDD = errors.union(warnings).union(exceptions)

    //val tempRDD=badlinesRDD.take(70)
    val tempRDD = badlinesRDD.collect()

    println(tempRDD.mkString("\n"))

  }

  /*  def isMatch(s: String, query:String): Boolean = {
    s.contains(query)
  }*/

  def isMatch[s, query](s: String, query: String): Boolean = {
    s.contains(query)
  }
}