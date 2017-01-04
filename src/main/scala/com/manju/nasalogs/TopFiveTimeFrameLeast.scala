package com.manju.nasalogs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * @Author Manjunath Kempaiah
 * @Version 1.0
 * 
 * Write spark code to find out 5 time frames of least traffic (which day of the week or hour of the day receives least
 * traffic, this information will help company to do production deployment in that time frame so that less number of 
 * users will be affected if some thing goes wrong during deployment)
 * 
 */
object TopFiveTimeFrameLeast {
  
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object.
    val conf = new SparkConf().setAppName("Nasa - Top Five Time Frames Least").setMaster(args(0))
    val sc = new SparkContext(conf)
    
    //Reading the input file.
    val logFileRdd = sc.textFile(args(1))
    
    //Split and fetch only the timestamp (dd/mon/yyyy:hh) using the regular expression.
    val timestampRdd = logFileRdd.map(line => {
      val pattern = "[0-9]{2}/[a-zA-z]{3}/[0-9]{4}(:[0-9]{2})".r
      pattern.findFirstIn(line).get
    })
    
    //count the timestamp and sort the count by ascending order.
    val sortedTimestampCount = timestampRdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, true)
    
    //Printing the top five least time frames on the console.
    sortedTimestampCount.take(5).map(timestamp => {
      var fields = timestamp._1.split(":")
      "Day : " + fields(0) + " hour : " + fields(1)
    }).foreach(println)
  }  
}