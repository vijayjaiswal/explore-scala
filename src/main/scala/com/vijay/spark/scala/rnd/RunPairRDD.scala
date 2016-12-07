package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object RunPairRDD {

  def main(args: Array[String]): Unit = {
    println("Running Pair RDD...")

    val config = new SparkConf().setMaster("local").setAppName("PairRDD")
    val sc = new JavaSparkContext(config)
    sc.setLogLevel("FATAL")

    var pairRDD = new PairRDD(sc)
    //    pairRDD.creatPairRDD()
    //    pairRDD.aggregateByKey()
    //    pairRDD.cogroup()
    //    pairRDD.coGroupfilerRDD()
    //    pairRDD.creatPairRDD()
    //    pairRDD.dispayAllKeys()
    //    pairRDD.dispayAllValues()
    //    pairRDD.groupByKey()
    //    pairRDD.joinRDD()
    //    pairRDD.leftOuterJoin()
    //    pairRDD.reduceByKey()
    //    pairRDD.rightOuterJoin()
    //    pairRDD.sortByKey()
    //    pairRDD.subtractByKey()
    //pairRDD.foldByKey()
    //pairRDD.perKeyAverage()
    pairRDD.perKeyAverageCombiner()

  }

}