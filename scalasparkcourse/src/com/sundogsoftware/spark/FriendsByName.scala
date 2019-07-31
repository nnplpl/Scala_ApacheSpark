package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
  def parseLine(line: String) = {
    val friends = line.split(",")
    
    val friendsName = friends(1)
    
    val friendsNum = friends(3).toInt
    
    (friendsName, friendsNum)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","FriendsByName")
    
    val lines = sc.textFile("../fakefriends.csv")
    
    val rdd = lines.map(parseLine)
    
    val totalsFriends = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
    
    val averageFriends = totalsFriends.mapValues(x => (x._1/x._2))
    
    val result = averageFriends.collect()
    
    result.sorted.foreach(println)
  }
}