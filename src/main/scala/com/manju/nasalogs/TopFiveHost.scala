package com.manju.nasalogs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * @Author Manjunath Kempaiah
 * @Version 1.0
 * 
 * Write spark code to find out top 5 hosts / IP making the request along with count 
 * (This information will help company to find out locations where website is 
 * popular or to figure out potential DDoS attacks)
 * 
 * Sample output
 *
 * URL Count
 * 192.168.78.24 219
 * 
 */
object TopFiveHost {
  
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object.
    val conf = new SparkConf().setAppName("Nasa - Top Five Hosts").setMaster(args(0))
    val sc = new SparkContext(conf)
    
    //Reading the input file.
    val logFileRdd = sc.textFile(args(1))
    
    //Split and fetch only the hosts using regular expression
    val hostsRdd = logFileRdd.map(line => {
      val pattern = "(\\S)+".r
      pattern.findFirstIn(line).get
    })
    
    //count the hosts and sort the count by descending order.
    val sortedHostsCount = hostsRdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    
    //Printing the top five hosts along with count on the console.
    sortedHostsCount.take(5).map(host => host._1 + " " + host._2).foreach(println)
    
  }
}