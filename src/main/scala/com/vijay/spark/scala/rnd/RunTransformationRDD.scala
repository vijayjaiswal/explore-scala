package com.vijay.spark.scala.rnd

object RunTransformationRDD {
  def main(args: Array[String]): Unit = {
    println("Running Transformation RDD...")

    var transformationRDD = new TransformationRDD()
    transformationRDD.parseFiles()

  }

}