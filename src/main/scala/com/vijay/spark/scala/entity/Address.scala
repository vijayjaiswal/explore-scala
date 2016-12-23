package com.vijay.spark.scala.entity

class Address(streetAddress: String, city: String, state: String, postalCode: String) extends Serializable {
  val strAddress=streetAddress
  val cityName=city
  val stateName=state
  val postCode=postalCode
}