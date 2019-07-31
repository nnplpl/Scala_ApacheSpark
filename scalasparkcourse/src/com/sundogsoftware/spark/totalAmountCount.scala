package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object totalAmountCount {
  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerID = fields(0)
    val amount = fields(2)
    (customerID, amount)
    
  }
  
  def main(args : Array[String]) = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "totalAmountCount")
    
    val data = sc.textFile("../customer-orders.csv")
    
    val parsedLine = data.map(parseLine)
    
    val countResult = parsedLine.map(x => (x._1.toInt, x._2.toFloat))
    
    val totalAmount = countResult.reduceByKey((x,y) => (x+y))
    
     val totalmap = totalAmount.map(x => (x._2,x._1))
    
    // val resultSorted = totalmap.sortByKey()
    
    val resultTotal = totalmap.collect()
    
    
    resultTotal.sorted.foreach(println)
  }
}