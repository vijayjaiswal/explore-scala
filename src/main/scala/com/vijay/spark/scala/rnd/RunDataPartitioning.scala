package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object RunDataPartitioning {
  def main(args: Array[String]): Unit = {
    println("Running Data Partitioning...")

    val config = new SparkConf().setMaster("local").setAppName("DataPartitioning")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    val sc = new JavaSparkContext(config)
    sc.setLogLevel("FATAL")
    val dataPartitioning = new DataPartitioning(sc)
    
   dataPartitioning.createUserFile
  }
}