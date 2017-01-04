package com.manju.nasalogs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * @Author Manjunath Kempaiah
 * @Version 1.0
 * 
 * Write spark code to find out top 5 time frame for high traffic ( 
 * which day of the week or hour of the day receives peak traffic, this 
 * information will help company to manage resources for handling peak traffic load)
 * 
 */
object TopFiveTimeFramePeak {
  
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object.
    val conf = new SparkConf().setAppName("Nasa - Top Five Time Frames Peak").setMaster(args(0))
    val sc = new SparkContext(conf)
    
    //Reading the input file.
    val logFileRdd = sc.textFile(args(1))
    
    //Split and fetch only the timestamp (dd/mon/yyyy:hh) using regular expression.
    val timestampRdd = logFileRdd.map(line => {
      val pattern = "[0-9]{2}/[a-zA-z]{3}/[0-9]{4}(:[0-9]{2})".r
      pattern.findFirstIn(line).get
    })
    
    //count the timestamp and sort the count by descending order.
    val sortedTimestampCount = timestampRdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    
    //Printing the top five peak times frames on the console.
    sortedTimestampCount.take(5).map(timestamp => {
      var fields = timestamp._1.split(":")
      "Day : " + fields(0) + " hour : " + fields(1)
    }).foreach(println)
  }  
}