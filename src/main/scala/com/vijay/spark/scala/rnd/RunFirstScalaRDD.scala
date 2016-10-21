package com.vijay.spark.scala.rnd

object RunFirstScalaRDD {
  def main(args: Array[String]): Unit = {
    println("Running First Scala RDD...")
    
    var firstScalaRDD = new FirstScalaRDD()
    firstScalaRDD.printWord("panda")
    
    
  }
}