package com.vijay.spark.scala.rnd

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

class SortRDD(sc: JavaSparkContext) {
  val numberList = sc.parallelize(List(1, 2, 5, 9, 4, 7, 6), 2)
  val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)

  def sortBy() {
    println("===================== sortBy =====================")
    numberList.sortBy(c => c, true, 2).foreach(println)
  }

  def sortByDesc() {
    println("===================== sortByDesc =====================")
    numberList.sortBy(c => c, false, 2).foreach(println)
  }

  def takeOrdered() {
    println("===============++=== takeOrdered ================++==")
    numberList.takeOrdered(numberList.count().toInt).foreach(println)
  }

  def takeOrderedDesc() {
    println("================== takeOrderedDesc ==================")
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = -1 * a.toString.compare(b.toString)
    }
    numberList.takeOrdered(numberList.count().toInt)(sortIntegersByString).foreach(println)
  }

  def sort() {
    println("===================== sort =====================")

    /* implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) =  a.toString.compare(b.toString)
    }*/
    pairRDD.sortByKey().foreach(println)
  }
  def sortDesc() {
    println("===================== sortDesc =====================")

    /* implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) =  -1*(a.toString.compare(b.toString))
    }*/
    pairRDD.sortByKey(false, 2).foreach(println)
  }

}