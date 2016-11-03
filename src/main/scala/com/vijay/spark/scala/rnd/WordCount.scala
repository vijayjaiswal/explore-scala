package com.vijay.spark.scala.rnd

import org.apache.spark.rdd.RDD

class WordCount(wordCountRDD: RDD[String]) {
  val count = wordCountRDD.flatMap(x => x.split(" ")).countByValue()
  count.foreach(println)
}