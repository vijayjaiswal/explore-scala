package com.vijay.spark.scala.rnd

object RunPairRDD {

  def main(args: Array[String]): Unit = {
    println("Running Pair RDD...")

    var pairRDD = new PairRDD()
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
    pairRDD.foldByKey()

  }

}