package com.manju.recommendation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.spark.ml.evaluation.RegressionEvaluator 
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

/*
 * Class finds the recommended movies for the user.
 * Automated approach - using code to generated my movies along with ratings.
 */
object MovieRecommendationAuto {
  
  /*
   * Method converts the String value by Spliting using '::' character into
   * Rating Class.
   */
  def parseRating(str: String): Rating = {  
    val fields = str.split("::")  
    assert(fields.size == 4)  
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat) 
  }
  
  /*
   * Method maps the sample movie ids into my user id along with some random
   * ratings and also the timestamp.
   */
  def createUserRatingsFromSampleMovieIds(sampleMovieIds:Array[Int], myUserId:Int):ListBuffer[Rating] = {
    val myMovieRatings = ListBuffer[Rating]();
    val random = Random
    for(movieId <- sampleMovieIds){
      var randValue = random.nextInt(5)
      if(randValue == 0) randValue = randValue + 1
      myMovieRatings += Rating(myUserId, movieId, randValue)
    }
    return myMovieRatings
  }

  /*
   * Main method where the program execution begins
   */  
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object
    val conf = new SparkConf().setAppName("Movie Recommendation Automated of adding my movies along with ratings").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Loading the ratings file from HDFS
    val ratingsRdd = sc.textFile("/data/ml-1m/ratings.dat")
    val ratings = ratingsRdd.map(parseRating)
    ratings.persist
    
    //Fetch maximum userid from the ratings and add one to it.
    val userIdsList = ratings.map(rating => rating.user)
    val myUserId = userIdsList.max + 1
    
    //Fetching sample movies Ids from the movieIds List to map to myuserId
    val movieIdsList = ratings.map(rating => rating.product)
    val sampleMovieIds = movieIdsList.sample(false, 0.0001).collect()
    
    //Mapping sample movie ids into my userId along with random ratings
    val myMovieRatings = createUserRatingsFromSampleMovieIds(sampleMovieIds, myUserId);
    myMovieRatings.foreach(println)
    
    //Merging my movie ratings along with other ratings in order to do movie recommendation for my self
    val myMovieRatingsRdd = sc.parallelize(myMovieRatings)
    val mergedRatings = ratings.union(myMovieRatingsRdd)
    
    //Creating ALS class from spark mlib package in order to train the model.
    val model = ALS.train(mergedRatings, 15, 10)
    
    //Calling recommendProducts method to fetch the recommended movies for my user id.
    val recommendedMovies = model.recommendProducts(myUserId, 20);
    
    //Loading movies.dat to fetch the movie names
    val moviesRdd = sc.textFile("/data/ml-1m/movies.dat")
    val movies = moviesRdd.map(line => {
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }).collectAsMap()
    
    //Finally printing the movies recommended for my user Id.
    recommendedMovies.foreach { rating => println("Movie Id : " + rating.product + " -> " + movies(rating.product)) }
  }
}