package com.vijay.spark.scala.rnd

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Array

class PairRDD extends Serializable {
  val config = new SparkConf().setMaster("local").setAppName("PairRDD")
  val sc = new JavaSparkContext(config)
  sc.setLogLevel("FATAL")
  val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)
  val pairRDDSecond = sc.parallelize(List(("cat", 2), ("donkey", 12), ("dog", 3), ("mouse", 1)), 2)

  def creatPairRDD() {
    println("Invoked creatPairRDD....")

    /*  val file = sc.parallelize(List("a 1", "a 2", "b 2", "a 3", "c 2", "c 2"))
    val lines = file.map(x => (x.split(" ")(0), x.split(" ")(1)))
    lines.foreach(println)
    println("Starting by ReduceByKey...")
    val result=lines.reduceByKey((x, y) => x+y)
    result.foreach(println)*/

    println("-------------------------------\n")
    pairRDD.foreach(println)
    println("-------------------------------\n")
    pairRDDSecond.foreach(println)
    println("-------------------------------\n")
  }
  def reduceByKey() {
    //reduceByKey
    println("-------------------------------\n ReduceByKey Starting..")
    val lines1 = pairRDD.reduceByKey((x, y) => x + y)
    lines1.foreach(println)
  }
  
   def foldByKey(){
    //reduceByKey
    println("-------------------------------\n FoldByKey Starting..")
    //val lines = pairRDD.foldByKey(("") (_+ _))
    val lines = pairRDD.map(x => (x._1, x._2))
    lines.foldByKey(0)(_ + _).collect
    lines.foreach(println)
  }
  def groupByKey() {
    //groupByKey
    println("-------------------------------\n groupByKey Starting..")
    val lines2 = pairRDD.groupByKey()
    lines2.foreach(println)
  }

  def aggregateByKey() {
    //AggregateByKey
    println("-------------------------------\n AggregateByKey Starting..")
    val lines3 = pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect
    lines3.foreach(println)
  }

  def dispayAllKeys() {
    //Key
    println("-------------------------------\n Key Starting..")
    val lines4 = pairRDD.keys
    lines4.foreach(println)
  }
  def dispayAllValues() {
    //Values
    println("-------------------------------\n Values Starting..")
    val lines5 = pairRDD.values
    lines5.foreach(println)
  }

  def sortByKey() {
    //sortByKey
    println("-------------------------------\n SortByKey Starting..")
    val lines6 = pairRDD.sortByKey()
    lines6.foreach(println)
  }

  def subtractByKey() {
    //subtractByKey
    println("-------------------------------\n SubtractByKey Starting..")
    val lines7 = pairRDDSecond.subtractByKey(pairRDD)
    lines7.foreach(println)
  }

  def joinRDD() {
    //join
    println("-------------------------------\n Join Starting..")
    val lines8 = pairRDDSecond.join(pairRDD)
    lines8.foreach(println)
  }
  def rightOuterJoin() {
    //RightOuterJoin
    println("-------------------------------\n RightOuterJoin Starting..")
    val lines9 = pairRDDSecond.rightOuterJoin(pairRDD)
    lines9.foreach(println)
  }

  def leftOuterJoin() {
    //LeftOuterJoin
    println("-------------------------------\n LeftOuterJoin Starting..")
    val lines10 = pairRDDSecond.leftOuterJoin(pairRDD)
    lines10.foreach(println)
  }

  def cogroup() {
    //cogroup
    println("-------------------------------\n Cogroup Starting..")
    val lines11 = pairRDDSecond.cogroup(pairRDD)
    lines11.foreach(println)
  }

  def coGroupfilerRDD() {
    println("-------------------------------\n Cogroup Filter Starting..")
    val lines11 = pairRDDSecond.cogroup(pairRDD)
  //  lines11.foreach(println)
    val r = lines11.filter { case (key, value) => key == "cat" }
    println(r.take(1))

  }

}