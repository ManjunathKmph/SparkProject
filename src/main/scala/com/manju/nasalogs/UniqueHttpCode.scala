package com.manju.nasalogs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * @Author Manjunath Kempaiah
 * @Version 1.0
 * 
 * Write spark code to find out unique HTTP codes returned by the server along with count 
 * (this information is helpful for devops team to find out how many requests are failing so that
 * appropriate action can be taken to fix the issue)
 * 
 * Sample output -
 *
 * HTTP code - Count
 * 200 - 15400
 * 404 - 324 
 */
object UniqueHttpCode {
  
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object.
    val conf = new SparkConf().setAppName("Nasa - Unique Http Codes").setMaster(args(0))
    val sc = new SparkContext(conf)
    
    //Reading the input file.
    val logFileRdd = sc.textFile(args(1))
    
    //Split and fetch only the http codes using regular expression.
    val httpCodesRdd = logFileRdd.map(line => {
      val pattern = "[0-9]{3}\\s[0-9\\-]+$".r
       val fields = pattern.findFirstIn(line).get.split(" ")
       fields(0).toInt
    })
    
    //count the http codes and sort count in descending order.
    val httpCodesCount = httpCodesRdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    
    //Printing the http codes along with count on the console.
    httpCodesCount.map(httpCode => httpCode._1 + " - " + httpCode._2).foreach(println)
    
  }
  
}