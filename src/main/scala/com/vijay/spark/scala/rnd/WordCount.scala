package com.vijay.spark.scala.rnd

import org.apache.spark.rdd.RDD

class WordCount(wordCountRDD: RDD[String]) {

  /*  val words = input.flatMap(x => x.split(" "))
			val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
  */
  val count = wordCountRDD.flatMap(x => x.split(" ")).countByValue()
  count.foreach(println)
}