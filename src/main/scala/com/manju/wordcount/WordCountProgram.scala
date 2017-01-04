package com.manju.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCountProgram {
  
  /*
   * Removing the non - alphanumeric characters and converting to lower case.
   */
  def replaceNonAlphanumeric(word:String):(String, Int) = {
  	var tempword = word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase
  	return (tempword, 1)
  }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count").setMaster(args(0))
    val sc = new SparkContext(conf)
    val input = args(1)
    val output = args(2)
    val wc = sc.textFile(input).flatMap(line => line.split(" ")).
              map(replaceNonAlphanumeric).reduceByKey(_+_)
    wc.saveAsTextFile(output)
    
  }
}