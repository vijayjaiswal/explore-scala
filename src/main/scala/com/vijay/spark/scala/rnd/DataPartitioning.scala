package com.vijay.spark.scala.rnd

import org.apache.hadoop.io.Writable
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaSparkContext.toSparkContext

class DataPartitioning(sc: JavaSparkContext) {

  val userFileName = "E:\\Vijay\\Personnel\\RND\\Data\\DP\\UserInfo"

  def createUserFile() {
    println("===================== createUserFile =====================")
    val data = List(new UserInfo(new UserID(1), List("computer", "science")), new UserInfo(new UserID(2), List("science")))

    val data1 = sc.parallelize(data, 2)
    //data1.saveAsTextFile(userFileName)
    data1.saveAsObjectFile(userFileName)
    //saveAsSequenceFile("E:\\Vijay\\Personnel\\RND\\Data\\" + userFileName)
  }

  def joinRDDs() {
    println("===================== joinRDDs =====================")

   val userData = sc.objectFile(userFileName)
   // val userData = sc.textFile[UserID, UserInfo](userFileName)
    println(userData.name())
  }
}

class UserID(id: Int) extends Serializable   {
  val uid: Int = id
  /*def readFields(x$1: java.io.DataInput): Unit = this.readFields(x$1)
  def write(x$1: java.io.DataOutput): Unit = this.write(x$1)*/

}

class UserInfo(id: UserID, topic: List[String]) extends Serializable{
  val uid: UserID = id
  val topics: List[String] = topic
 /* def readFields(x$1: java.io.DataInput): Unit = this.readFields(x$1)
  def write(x$1: java.io.DataOutput): Unit = this.write(x$1)*/
}
