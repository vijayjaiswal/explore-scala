package com.vijay.spark.scala.rnd

object RunScalaOperationsRDD {
  def main(args: Array[String]): Unit = {
    println("Running Scala Operations RDD...")

    var scalaOperationsRDD = new ScalaOperationsRDD()
    //scalaOperationsRDD.processTransformations()
    //scalaOperationsRDD.processActions()
    scalaOperationsRDD.processPersistence()
  }
}