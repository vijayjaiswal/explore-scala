package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.storage.StorageLevel

class ScalaOperationsRDD {

  val config = new SparkConf().setMaster("local").setAppName("ScalaOperationsRDD")

  val sc = new JavaSparkContext(config)
  //sc.setLogLevel("WARN")
  sc.setLogLevel("INFO")

  val list1 = sc.parallelize(List(1, 2, 3, 4), 3)
  val list2 = sc.parallelize(List(3, 4, 5, 6), 3)
  val list3 = List(3, 4, 5)

  def processTransformations() {

    println("\nSquare of List 1.......")
    var result = list1.map(x => x * x)
    println(result.collect().mkString("|"))

    println("\nUnion.......")
    result = list1.union(list2)
    println(result.collect().mkString(","))

    println("\nIntersection.......")
    result = list1.intersection(list2)
    println(result.collect().mkString(","))

    println("\nSubstract.......")
    result = list1.subtract(list2)
    println(result.collect().mkString(","))

    println("\nCartesian.......")
    println((list1.cartesian(list2).collect()).mkString(","))

  }

  def processActions() {
    println("\nReduce(Sum).......")
    val sum = list1.reduce((x, y) => x + y)
    println(sum)

    println("\nFold(Sum).......")
    val foldSum = list2.fold(0)((x, y) => x + y)
    println(foldSum)

    println("\nAggregate.......")
    val result = list1.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    println("result=" + result)
    val avg = result._1 / result._2.toDouble
    println("avg=" + avg)

    println("\nTake.......")
    var temp = list2.take(3)
    println(temp.mkString(","))

    println("\nTop.......")
    temp = list2.top(3)
    println(temp.mkString(","))

    println("\nTakeSample.......")
    temp = list2.takeSample(false, 3)
    println(temp.mkString(","))

    println("\nforeach.......")
    list2.foreach(x => println("square of " + x + " is " + x * x))

  }

  def processPersistence() {
    val result = list1.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY)
    println(result.count())
    println(result.collect().mkString(","))

  }
}