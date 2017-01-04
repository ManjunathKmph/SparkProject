package com.manju.nasalogs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * @Author Manjunath Kempaiah
 * @Version 1.0
 * 
 * Write spark code to find out top 10 requested URLs along with count of number 
 * of times they have been requested (This information will help company to find 
 * out most popular pages and how frequently they are accessed)
 * 
 * Sample output
 *
 * URL Count
 * shuttle/missions/sts-71/mission-sts-71.html 549
 * shuttle/resources/orbiters/enterprise.html 145
 * 
 */
object TopTenRequestUrl {
  
  /*
   * Method extracts the url from the requested url format by excluding http method type, http version.
   */
  def extractUrl(url:String): String = {
     var res = ""
     try {
       val pattern = "\\s?/[-a-zA-Z0-9@:%\\.\\/+~#=]*(\\.[a-z]{2,5}([-a-zA-Z0-9@:%\\ ,+.~#?&=]*))?".r
       res = pattern.findFirstIn(url).get.trim().toLowerCase()
     }catch {
       case e: Exception => println(url) //Printing the url which is not matched with the regular expression.
     }
     return res
  }
  
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object.
    val conf = new SparkConf().setAppName("Nasa - Top Ten Request Urls").setMaster(args(0))
    val sc = new SparkContext(conf)
    
    //Reading the input file.
    val logFileRdd = sc.textFile(args(1))
    
    //Split and fetch only the urls using regular expressions.
    val urlsRdd = logFileRdd.map(line => {
       val pattern = "\"(.)+\"".r
       val extractedRequestedUrl = pattern.findFirstIn(line).get
       extractUrl(extractedRequestedUrl)
    })
    
    //count the urls and sort the count by descending order.
    val sortedUrlsCount = urlsRdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)    
    
    //Printing the top ten requested urls along with count on the console.
    sortedUrlsCount.take(10).map(urlTuple => urlTuple._1 + " " + urlTuple._2).foreach(println)
    
  }
}