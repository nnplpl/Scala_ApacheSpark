package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaximumTemp {
  def parseLine(line: String)= {
    val fileds = line.split(",")
    val stationID = fileds(0)
    val entryType = fileds(2)
    val temperature = (fileds(3).toFloat)*0.1f*(9.0f/5.0f)+32.0F
    (stationID, entryType, temperature)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","MaximumTemp")
    
    val temp = sc.textFile("../1800.csv")
    
    val parsedLines = temp.map(parseLine)
    
    val maxTemp = parsedLines.filter(x => x._2 == "TMAX")
    
    val StationTemp = maxTemp.map(x => (x._1, x._3.toFloat))
    
    val maxTempByStation = StationTemp.reduceByKey((x,y) => max(x,y))
    
    val result = maxTempByStation.collect()
    
    for (r <- result.sorted){
      val station = r._1
      val temps = r._2
      val maxTemps = f"$temps%.2f F"
      println(s"$station maximum temperature is $maxTemps hahahhaha" )
    }
    
  }


}