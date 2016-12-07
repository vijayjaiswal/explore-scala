package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object RunSortRDD {
  def main(args: Array[String]): Unit = {
    println("Running Sort RDD...")

    val config = new SparkConf().setMaster("local").setAppName("SortRDD")
    val sc = new JavaSparkContext(config)
    sc.setLogLevel("FATAL")

    val sortRDD = new SortRDD(sc)
    sortRDD.sort
    sortRDD.sortDesc
    //sortRDD.takeOrdered
    //sortRDD.takeOrderedDesc
    //sortRDD.sortBy()
    //sortRDD.sortByDesc()
  }
}