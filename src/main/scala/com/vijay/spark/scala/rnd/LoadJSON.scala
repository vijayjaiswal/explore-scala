package com.vijay.spark.scala.rnd

import java.io.StringWriter

import org.apache.spark.SparkContext

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.vijay.spark.scala.entity.Customer
import com.vijay.spark.scala.entity.Person

class LoadJSON(sc: SparkContext) extends Serializable {

  val jsonPath = "E:\\Projects\\RND\\scala-workspace\\scalarnd\\src\\main\\scala\\com\\vijay\\spark\\scala\\rnd\\customer.json"
  //val jsonPath = "E:\\Projects\\RND\\scala-workspace\\scalarnd\\src\\main\\scala\\com\\vijay\\spark\\scala\\rnd\\person.json"
  val outputFile = "E:\\Projects\\RND\\scala-workspace\\scalarnd\\src\\main\\scala\\com\\vijay\\spark\\scala\\rnd\\output"
  val input = sc.textFile(jsonPath)
  //val source: String = Source.fromFile(jsonPath).getLines.mkString
  /*val source: String = Source.fromFile(jsonPath).getLines.mkString
  println(source)*/

  def loadCustomerJson() {
    val result = input.mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      records.flatMap(record => {
        try {
          Some(mapper.readValue(record, classOf[Customer]))
          // Some(mapper.readValue(record, classOf[Person]))
        } catch {
          case e: Exception => None
        }
      })
    }, true)

    result.foreach(c => {
      println(c.fName)
    })

    //result.saveAsTextFile(outputFile)
    result.mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
    
      records.map(mapper.writeValueAsString(_))
      
    }).saveAsTextFile(outputFile)
  }

  def personJson() {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val person = Person("fred", 25)
    val out = new StringWriter
    mapper.writeValue(out, person)

    val json = out.toString()
    println(json)

    val person2 = mapper.readValue(json, classOf[Person])
    println(person2)
  }

}