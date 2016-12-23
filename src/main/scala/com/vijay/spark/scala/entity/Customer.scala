package com.vijay.spark.scala.entity

//class Customer (firstName: String, lastName: String, age: Int, address: Address, phones: Array[PhoneNumber]) extends Serializable{
class Customer(firstName: String, lastName: String, age: Int, address: Address, phones: Array[PhoneNumber]) extends Serializable {
  val fName = firstName
  val lName = lastName
  val customerAge = age
  val homeAddress = address
  val cellPhones = phones

}